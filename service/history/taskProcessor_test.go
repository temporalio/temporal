// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	taskForTest struct {
		tasks.Key
	}

	taskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockProcessor *MocktimerProcessor

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
	mockShard := shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		tests.NewDynamicConfig(),
	)
	s.mockShard = mockShard

	s.mockProcessor = NewMocktimerProcessor(s.controller)

	s.logger = s.mockShard.GetLogger()

	s.scopeIdx = 0
	s.scope = metrics.NewNoopMetricsClient().Scope(s.scopeIdx)
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
	s.mockShard.StopForTest()
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_ShutDown() {
	close(s.taskProcessor.shutdownCh)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		&taskInfo{
			processor: s.mockProcessor,
			Task:      &taskForTest{Key: tasks.Key{TaskID: 12345, FireTime: time.Now().UTC()}},
			attempt:   1,
		},
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_NamespaceErrRetry_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &taskForTest{Key: tasks.Key{TaskID: 12345, FireTime: time.Now().UTC()}}, s.logger)
	var taskFilterErr taskFilter = func(task tasks.Task) (bool, error) {
		return false, errors.New("some random error")
	}
	var taskFilter taskFilter = func(task tasks.Task) (bool, error) {
		return true, nil
	}
	s.mockProcessor.EXPECT().getTaskFilter().Return(taskFilterErr)
	s.mockProcessor.EXPECT().getTaskFilter().Return(taskFilter)
	s.mockProcessor.EXPECT().process(context.Background(), task).Return(s.scopeIdx, nil)
	s.mockProcessor.EXPECT().complete(task)
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_NamespaceFalse_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &taskForTest{Key: tasks.Key{TaskID: 12345, FireTime: time.Now().UTC()}}, s.logger)
	task.shouldProcessTask = false
	var taskFilter taskFilter = func(task tasks.Task) (bool, error) {
		return false, nil
	}
	s.mockProcessor.EXPECT().getTaskFilter().Return(taskFilter)
	s.mockProcessor.EXPECT().process(context.Background(), task).Return(s.scopeIdx, nil)
	s.mockProcessor.EXPECT().complete(task)
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_NamespaceTrue_ProcessNoErr() {
	task := newTaskInfo(s.mockProcessor, &taskForTest{Key: tasks.Key{TaskID: 12345, FireTime: time.Now().UTC()}}, s.logger)
	var taskFilter taskFilter = func(task tasks.Task) (bool, error) {
		return true, nil
	}
	s.mockProcessor.EXPECT().getTaskFilter().Return(taskFilter)
	s.mockProcessor.EXPECT().process(context.Background(), task).Return(s.scopeIdx, nil)
	s.mockProcessor.EXPECT().complete(task)
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestProcessTaskAndAck_NamespaceTrue_ProcessErrNoErr() {
	err := errors.New("some random err")
	task := newTaskInfo(s.mockProcessor, &taskForTest{Key: tasks.Key{TaskID: 12345, FireTime: time.Now().UTC()}}, s.logger)
	var taskFilter taskFilter = func(task tasks.Task) (bool, error) {
		return true, nil
	}
	s.mockProcessor.EXPECT().getTaskFilter().Return(taskFilter)
	s.mockProcessor.EXPECT().process(context.Background(), task).Return(s.scopeIdx, err)
	s.mockProcessor.EXPECT().process(context.Background(), task).Return(s.scopeIdx, nil)
	s.mockProcessor.EXPECT().complete(task)
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).Times(2)
	s.taskProcessor.processTaskAndAck(
		s.notificationChan,
		task,
	)
}

func (s *taskProcessorSuite) TestHandleTaskError_EntityNotExists() {
	err := serviceerror.NewNotFound("")

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_ErrTaskRetry() {
	err := consts.ErrTaskRetry
	delay := time.Second

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	go func() {
		time.Sleep(delay)
		s.notificationChan <- struct{}{}
	}()

	err = s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err)
	duration := time.Since(taskInfo.startTime)
	s.True(duration >= delay)
	s.Equal(consts.ErrTaskRetry, err)
}

func (s *taskProcessorSuite) TestHandleTaskError_ErrTaskDiscarded() {
	err := consts.ErrTaskDiscarded

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))
}

func (s *taskProcessorSuite) TestHandleTaskError_NamespaceNotActiveError() {
	err := serviceerror.NewNamespaceNotActive("", "", "")

	taskInfo := newTaskInfo(s.mockProcessor, nil, s.logger)
	taskInfo.startTime = time.Now().UTC().Add(-namespace.CacheRefreshInterval * time.Duration(3))
	s.Nil(s.taskProcessor.handleTaskError(s.scope, taskInfo, s.notificationChan, err))

	taskInfo.startTime = time.Now().UTC()
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

func (t *taskForTest) GetKey() tasks.Key {
	return t.Key
}

func (t *taskForTest) GetTaskID() int64 {
	return t.TaskID
}

func (t *taskForTest) GetVisibilityTime() time.Time {
	return t.FireTime
}

func (t *taskForTest) GetNamespaceID() string {
	return tests.NamespaceID.String()
}

func (t *taskForTest) GetWorkflowID() string {
	return tests.WorkflowID
}

func (t *taskForTest) GetRunID() string {
	return tests.RunID
}

func (t *taskForTest) GetVersion() int64             { panic("implement me") }
func (t *taskForTest) SetVersion(_ int64)            { panic("implement me") }
func (t *taskForTest) SetTaskID(_ int64)             { panic("implement me") }
func (t *taskForTest) SetVisibilityTime(_ time.Time) { panic("implement me") }
