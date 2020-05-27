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
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	t "github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

type (
	queueTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.TestContext
		mockQueueTaskExecutor *MockExecutor
		mockQueueTaskInfo     *MockInfo

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
		&persistence.ShardInfo{
			ShardID: 10,
			RangeID: 1,
		},
		config.NewForTest(),
	)
	s.mockQueueTaskExecutor = NewMockExecutor(s.controller)
	s.mockQueueTaskInfo = NewMockInfo(s.controller)

	s.scope = metrics.NewClient(tally.NoopScope, metrics.History).Scope(0)
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.timeSource = clock.NewRealTimeSource()
	s.maxRetryCount = dynamicconfig.GetIntPropertyFn(10)
}

func (s *queueTaskSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *queueTaskSuite) TestExecute_TaskFilterErr() {
	taskFilterErr := errors.New("some random error")
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return false, taskFilterErr
	})
	err := taskBase.Execute()
	s.Equal(taskFilterErr, err)
}

func (s *queueTaskSuite) TestExecute_ExecutionErr() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	executionErr := errors.New("some random error")
	s.mockQueueTaskExecutor.EXPECT().Execute(taskBase.Info, true).Return(executionErr).Times(1)

	err := taskBase.Execute()
	s.Equal(executionErr, err)
}

func (s *queueTaskSuite) TestExecute_Success() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	s.mockQueueTaskExecutor.EXPECT().Execute(taskBase.Info, true).Return(nil).Times(1)

	err := taskBase.Execute()
	s.NoError(err)
}

func (s *queueTaskSuite) TestHandleErr_ErrEntityNotExists() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := &workflow.EntityNotExistsError{}
	s.NoError(taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrTaskRetry() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := ErrTaskRetry
	s.Equal(ErrTaskRetry, taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrTaskDiscarded() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := ErrTaskDiscarded
	s.NoError(taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrDomainNotActive() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := &workflow.DomainNotActiveError{}

	taskBase.submitTime = time.Now().Add(-cache.DomainCacheRefreshInterval * time.Duration(2))
	s.NoError(taskBase.HandleErr(err))

	taskBase.submitTime = time.Now()
	s.Equal(err, taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_ErrCurrentWorkflowConditionFailed() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := &persistence.CurrentWorkflowConditionFailedError{}
	s.NoError(taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestHandleErr_UnknownErr() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	err := errors.New("some random error")
	s.Equal(err, taskBase.HandleErr(err))
}

func (s *queueTaskSuite) TestTaskState() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	s.Equal(t.TaskStatePending, taskBase.State())

	taskBase.Ack()
	s.Equal(t.TaskStateAcked, taskBase.State())

	taskBase.Nack()
	s.Equal(t.TaskStateNacked, taskBase.State())
}

func (s *queueTaskSuite) TestTaskPriority() {
	taskBase := s.newTestQueueTaskBase(func(task Info) (bool, error) {
		return true, nil
	})

	priority := 10
	taskBase.SetPriority(priority)
	s.Equal(priority, taskBase.Priority())
}

func (s *queueTaskSuite) newTestQueueTaskBase(
	taskFilter Filter,
) *taskBase {
	return newQueueTaskBase(
		s.mockShard,
		s.mockQueueTaskInfo,
		QueueTypeActiveTransfer,
		s.scope,
		s.logger,
		taskFilter,
		s.mockQueueTaskExecutor,
		s.timeSource,
		s.maxRetryCount,
	)
}
