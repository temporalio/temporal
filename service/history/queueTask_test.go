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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/service/history/shard"
)

type (
	queueTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockQueueTaskExecutor *MockqueueTaskExecutor
		mockQueueTaskInfo     *MockqueueTaskInfo

		scope         metrics.Scope
		logger        log.Logger
		timeSource    clock.TimeSource
		maxRetryCount dynamicconfig.IntPropertyFn
	}
)

func TestQueueTaskSuite(t *testing.T) {
	s := new(queueTaskSuite)
	suite.Run(t, s)
}

func (s *queueTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 10,
				RangeId: 1,
			}},
		NewDynamicConfigForTest(),
	)
	s.mockQueueTaskExecutor = NewMockqueueTaskExecutor(s.controller)
	s.mockQueueTaskInfo = NewMockqueueTaskInfo(s.controller)

	s.scope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(0)
	s.logger = log.NewDevelopment()
	s.timeSource = clock.NewRealTimeSource()
	s.maxRetryCount = dynamicconfig.GetIntPropertyFn(10)
}

func (s *queueTaskSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *queueTaskSuite) TestExecute_TaskFilterErr() {
	taskFilterErr := errors.New("some random error")
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return false, taskFilterErr
	})
	err := queueTaskBase.Execute()
	s.Equal(taskFilterErr, err)
}

func (s *queueTaskSuite) TestExecute_ExecutionErr() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	executionErr := errors.New("some random error")
	s.mockQueueTaskExecutor.EXPECT().execute(queueTaskBase.queueTaskInfo, true).Return(executionErr)

	err := queueTaskBase.Execute()
	s.Equal(executionErr, err)
}

func (s *queueTaskSuite) TestExecute_Success() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	s.mockQueueTaskExecutor.EXPECT().execute(queueTaskBase.queueTaskInfo, true).Return(nil)

	err := queueTaskBase.Execute()
	s.NoError(err)
}

func (s *queueTaskSuite) TestHandleErr_ErrEntityNotExists() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := &serviceerror.NotFound{}
	s.NoError(queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrTaskRetry() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := ErrTaskRetry
	s.Equal(ErrTaskRetry, queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrTaskDiscarded() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := ErrTaskDiscarded
	s.NoError(queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrNamespaceNotActive() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := &serviceerror.NamespaceNotActive{}

	queueTaskBase.submitTime = time.Now().UTC().Add(-cache.NamespaceCacheRefreshInterval * time.Duration(2))
	s.NoError(queueTaskBase.HandleErr(err))

	queueTaskBase.submitTime = time.Now().UTC()
	s.Equal(err, queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrCurrentWorkflowConditionFailed() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := &persistence.CurrentWorkflowConditionFailedError{}
	s.NoError(queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_UnknownErr() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	err := errors.New("some random error")
	s.Equal(err, queueTaskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestTaskState() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	s.Equal(task.TaskStatePending, queueTaskBase.State())

	queueTaskBase.Ack()
	s.Equal(task.TaskStateAcked, queueTaskBase.State())

	queueTaskBase.Nack()
	s.Equal(task.TaskStateNacked, queueTaskBase.State())
}

func (s *queueTaskSuite) TestTaskPriority() {
	queueTaskBase := s.newTestQueueTaskBase(func(task queueTaskInfo) (bool, error) {
		return true, nil
	})

	priority := 10
	queueTaskBase.SetPriority(priority)
	s.Equal(priority, queueTaskBase.Priority())
}

func (s *queueTaskSuite) newTestQueueTaskBase(
	taskFilter taskFilter,
) *queueTaskBase {
	return newQueueTaskBase(
		s.mockShard,
		s.mockQueueTaskInfo,
		s.scope,
		s.logger,
		taskFilter,
		s.mockQueueTaskExecutor,
		s.timeSource,
		s.maxRetryCount,
	)
}
