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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

type (
	queueTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.TestContext
		mockPriorityAssigner *MockPriorityAssigner

		metricsClient metrics.Client
		logger        log.Logger

		processor *processorImpl
	}

	mockQueueTaskMatcher struct {
		task *MockTask
	}
)

func TestQueueTaskProcessorSuite(t *testing.T) {
	s := new(queueTaskProcessorSuite)
	suite.Run(t, s)
}

func (s *queueTaskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID: 10,
			RangeID: 1,
		},
		config.NewForTest(),
	)
	s.mockPriorityAssigner = NewMockPriorityAssigner(s.controller)

	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)

	s.processor = s.newTestQueueTaskProcessor()
}

func (s *queueTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *queueTaskProcessorSuite) TestIsRunning() {
	s.False(s.processor.isRunning())

	s.processor.Start()
	s.True(s.processor.isRunning())

	s.processor.Stop()
	s.False(s.processor.isRunning())
}

func (s *queueTaskProcessorSuite) TestGetOrCreateShardTaskScheduler_ProcessorNotRunning() {
	scheduler, err := s.processor.getOrCreateShardTaskScheduler(s.mockShard)
	s.Equal(errTaskProcessorNotRunning, err)
	s.Nil(scheduler)
}

func (s *queueTaskProcessorSuite) TestGetOrCreateShardTaskScheduler_ShardProcessorAlreadyExists() {
	mockScheduler := task.NewMockScheduler(s.controller)
	s.processor.shardSchedulers[s.mockShard] = mockScheduler

	s.processor.Start()
	scheduler, err := s.processor.getOrCreateShardTaskScheduler(s.mockShard)
	s.NoError(err)
	s.Equal(mockScheduler, scheduler)
}

func (s *queueTaskProcessorSuite) TestGetOrCreateShardTaskScheduler_ShardProcessorNotExist() {
	s.Empty(s.processor.shardSchedulers)

	s.processor.Start()
	scheduler, err := s.processor.getOrCreateShardTaskScheduler(s.mockShard)
	s.NoError(err)

	s.Len(s.processor.shardSchedulers, 1)
	scheduler.Stop()
}

func (s *queueTaskProcessorSuite) TestStopShardProcessor() {
	s.Empty(s.processor.shardSchedulers)
	s.processor.StopShardProcessor(s.mockShard)

	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().Stop().Times(1)
	s.processor.shardSchedulers[s.mockShard] = mockScheduler

	s.processor.StopShardProcessor(s.mockShard)
	s.Empty(s.processor.shardSchedulers)
}

func (s *queueTaskProcessorSuite) TestStartStop() {
	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().Start().Times(1)
	mockScheduler.EXPECT().Stop().Times(1)
	s.processor.hostScheduler = mockScheduler

	for i := 0; i != 10; i++ {
		mockShard := shard.NewTestContext(
			s.controller,
			&persistence.ShardInfo{
				ShardID: 10,
				RangeID: 1,
			},
			config.NewForTest(),
		)
		mockShardScheduler := task.NewMockScheduler(s.controller)
		mockShardScheduler.EXPECT().Stop().Times(1)
		s.processor.shardSchedulers[mockShard] = mockShardScheduler
	}

	s.processor.Start()
	s.processor.Stop()

	s.Empty(s.processor.shardSchedulers)
}

func (s *queueTaskProcessorSuite) TestSubmit() {
	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().GetShard().Return(s.mockShard).Times(1)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().TrySubmit(newMockQueueTaskMatcher(mockTask)).Return(false, nil).Times(1)

	mockShardScheduler := task.NewMockScheduler(s.controller)
	mockShardScheduler.EXPECT().Submit(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	s.processor.hostScheduler = mockScheduler
	s.processor.shardSchedulers[s.mockShard] = mockShardScheduler

	err := s.processor.Submit(mockTask)
	s.NoError(err)
}

func (s *queueTaskProcessorSuite) TestTrySubmit_AssignPriorityFailed() {
	mockTask := NewMockTask(s.controller)

	errAssignPriority := errors.New("some randome error")
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(errAssignPriority).Times(1)

	submitted, err := s.processor.TrySubmit(mockTask)
	s.Equal(errAssignPriority, err)
	s.False(submitted)
}

func (s *queueTaskProcessorSuite) TestTrySubmit_Fail() {
	mockTask := NewMockTask(s.controller)
	s.mockPriorityAssigner.EXPECT().Assign(newMockQueueTaskMatcher(mockTask)).Return(nil).Times(1)

	errTrySubmit := errors.New("some randome error")
	mockScheduler := task.NewMockScheduler(s.controller)
	mockScheduler.EXPECT().TrySubmit(newMockQueueTaskMatcher(mockTask)).Return(false, errTrySubmit).Times(1)

	s.processor.hostScheduler = mockScheduler

	submitted, err := s.processor.TrySubmit(mockTask)
	s.Equal(errTrySubmit, err)
	s.False(submitted)
}

func (s *queueTaskProcessorSuite) TestNewSchedulerOptions_UnknownSchedulerType() {
	options, err := newSchedulerOptions(0, 100, 10, 1, nil)
	s.Error(err)
	s.Nil(options)
}

func (s *queueTaskProcessorSuite) newTestQueueTaskProcessor() *processorImpl {
	config := config.NewForTest()
	processor, err := NewProcessor(
		s.mockPriorityAssigner,
		config,
		s.logger,
		s.metricsClient,
	)
	s.NoError(err)
	return processor.(*processorImpl)
}

func newMockQueueTaskMatcher(mockTask *MockTask) gomock.Matcher {
	return &mockQueueTaskMatcher{
		task: mockTask,
	}
}

func (m *mockQueueTaskMatcher) Matches(x interface{}) bool {
	taskPtr, ok := x.(*MockTask)
	if !ok {
		return false
	}
	return taskPtr == m.task
}

func (m *mockQueueTaskMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.task)
}
