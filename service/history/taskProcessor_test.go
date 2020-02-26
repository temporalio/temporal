// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	taskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shardContextTest

		mockProcessor *MockTimerProcessor

		scopeIdx         int
		scope            metrics.Scope
		logger           log.Logger
		notificationChan chan struct{}

		taskProcessor *taskProcessor
	}
)

func TestTaskProcessorSuite(t *testing.T) {
	s := new(taskProcessorSuite)
	suite.Run(t, s)
}

func (s *taskProcessorSuite) SetupSuite() {

}

func (s *taskProcessorSuite) TearDownSuite() {

}

func (s *taskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		NewDynamicConfigForTest(),
	)

	s.mockProcessor = &MockTimerProcessor{}

	s.logger = s.mockShard.GetLogger()

	s.scopeIdx = 0
	s.scope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(s.scopeIdx)
	s.notificationChan = make(chan struct{})
	h := &historyEngineImpl{
		shard:         s.mockShard,
		logger:        s.logger,
		metricsClient: s.mockShard.GetMetricsClient(),
	}
	options := taskProcessorOptions{
		queueSize:   s.mockShard.GetConfig().TimerTaskBatchSize() * s.mockShard.GetConfig().TimerTaskWorkerCount(),
		workerCount: s.mockShard.GetConfig().TimerTaskWorkerCount(),
	}
	s.taskProcessor = newTaskProcessor(options, s.mockShard, h.historyCache, s.logger)
}

func (s *taskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockProcessor.AssertExpectations(s.T())
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_ShutDown() {
	close(s.taskProcessor.shutdownCh)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		&taskInfo{
			processor: s.mockProcessor,
			task:      &persistence.TimerTaskInfo{},
		},
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_DomainErrRetry_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &persistence.TimerTaskInfo{TaskID: 12345, VisibilityTimestamp: time.Now()}, s.logger)
	var taskFilterErr taskFilter = func(task queueTaskInfo) (bool, error) {
		return false, errors.New("some random error")
	}
	var taskFilter taskFilter = func(task queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilterErr).Once()
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task).Return(s.scopeIdx, nil).Once()
	s.mockProcessor.On("complete", task).Once()
	s.mockShard.resource.DomainCache.EXPECT().GetDomainName(gomock.Any()).Return(testDomainName, nil).Times(1)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_DomainFalse_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &persistence.TimerTaskInfo{TaskID: 12345, VisibilityTimestamp: time.Now()}, s.logger)
	task.shouldProcessTask = false
	var taskFilter taskFilter = func(task queueTaskInfo) (bool, error) {
		return false, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task).Return(s.scopeIdx, nil).Once()
	s.mockProcessor.On("complete", task).Once()
	s.mockShard.resource.DomainCache.EXPECT().GetDomainName(gomock.Any()).Return(testDomainName, nil).Times(1)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_DomainTrue_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &persistence.TimerTaskInfo{TaskID: 12345, VisibilityTimestamp: time.Now()}, s.logger)
	var taskFilter taskFilter = func(task queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task).Return(s.scopeIdx, nil).Once()
	s.mockProcessor.On("complete", task).Once()
	s.mockShard.resource.DomainCache.EXPECT().GetDomainName(gomock.Any()).Return(testDomainName, nil).Times(1)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_DomainTrue_ProcessErrNoErr() {
	err := errors.New("some random err")
	task := newTaskInfo(s.mockProcessor, &persistence.TimerTaskInfo{TaskID: 12345, VisibilityTimestamp: time.Now()}, s.logger)
	var taskFilter taskFilter = func(task queueTaskInfo) (bool, error) {
		return true, nil
	}
	s.mockProcessor.On("getTaskFilter").Return(taskFilter).Once()
	s.mockProcessor.On("process", task).Return(s.scopeIdx, err).Once()
	s.mockProcessor.On("process", task).Return(s.scopeIdx, nil).Once()
	s.mockProcessor.On("complete", task).Once()
	s.mockShard.resource.DomainCache.EXPECT().GetDomainName(gomock.Any()).Return(testDomainName, nil).Times(2)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestHandleTaskError_EntityNotExists() {
	err := &workflow.EntityNotExistsError{}

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_ErrTaskRetry() {
	err := ErrTaskRetry
	delay := time.Second

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	go func() {
		time.Sleep(delay)
		s.notificationChan <- struct{}{}
	}()

	err = s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err)
	duration := time.Since(taskInfo.startTime)
	s.True(duration >= delay)
	s.Equal(ErrTaskRetry, err)
}

func (s *taskProcessorSuite) TestHandleTaskError_ErrTaskDiscarded() {
	err := ErrTaskDiscarded

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_DomainNotActiveError() {
	err := &workflow.DomainNotActiveError{}

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	taskInfo.startTime = time.Now().Add(-cache.DomainCacheRefreshInterval * time.Duration(2))
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))

	taskInfo.startTime = time.Now()
	s.Equal(err, s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_CurrentWorkflowConditionFailedError() {
	err := &persistence.CurrentWorkflowConditionFailedError{}

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_RandomErr() {
	err := errors.New("random error")

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Equal(err, s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}
