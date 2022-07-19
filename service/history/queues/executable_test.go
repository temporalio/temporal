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

package queues

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

const namespaceCacheRefreshInterval = 10 * time.Second

type (
	executableSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockExecutor    *MockExecutor
		mockScheduler   *MockScheduler
		mockRescheduler *MockRescheduler

		timeSource *clock.EventTimeSource
	}
)

func TestExecutableSuite(t *testing.T) {
	s := new(executableSuite)
	suite.Run(t, s)
}

func (s *executableSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockExecutor = NewMockExecutor(s.controller)
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockRescheduler = NewMockRescheduler(s.controller)

	s.timeSource = clock.NewEventTimeSource()
}

func (s *executableSuite) TearDownSuite() {
	s.controller.Finish()
}

func (s *executableSuite) TestExecute_TaskFiltered() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return false
	})

	s.NoError(executable.Execute())
}

func (s *executableSuite) TestExecute_TaskExecuted() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(metrics.NoopMetricsHandler, errors.New("some random error"))
	s.Error(executable.Execute())

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(metrics.NoopMetricsHandler, nil)
	s.NoError(executable.Execute())
}

func (s *executableSuite) TestExecute_UserLatency() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	expectedUserLatency := int64(133)
	updateContext := func(ctx context.Context, taskInfo interface{}) {
		metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency, expectedUserLatency)
	}

	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Do(updateContext).Return(metrics.NoopMetricsHandler, nil)
	s.NoError(executable.Execute())
	s.Equal(time.Duration(expectedUserLatency), executable.(*executableImpl).userLatency)
}

func (s *executableSuite) TestHandleErr_EntityNotExists() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.NoError(executable.HandleErr(serviceerror.NewNotFound("")))
}

func (s *executableSuite) TestHandleErr_ErrTaskRetry() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.Equal(consts.ErrTaskRetry, executable.HandleErr(consts.ErrTaskRetry))
}

func (s *executableSuite) TestHandleErr_ErrTaskDiscarded() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.NoError(executable.HandleErr(consts.ErrTaskDiscarded))
}

func (s *executableSuite) TestHandleErr_NamespaceNotActiveError() {
	now := time.Now().UTC()
	err := serviceerror.NewNamespaceNotActive("", "", "")

	s.timeSource.Update(now.Add(-namespaceCacheRefreshInterval * time.Duration(3)))
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})
	s.timeSource.Update(now)
	s.NoError(executable.HandleErr(err))

	executable = s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.Equal(err, executable.HandleErr(err))
}

func (s *executableSuite) TestHandleErr_RandomErr() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.Error(executable.HandleErr(errors.New("random error")))
}

func (s *executableSuite) TestTaskAck() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.Equal(ctasks.TaskStatePending, executable.State())

	executable.Ack()
	s.Equal(ctasks.TaskStateAcked, executable.State())
}

func (s *executableSuite) TestTaskNack_Resubmit() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.mockScheduler.EXPECT().TrySubmit(executable).Return(true, nil)

	executable.Nack(errors.New("some random error"))
	s.Equal(ctasks.TaskStatePending, executable.State())
}

func (s *executableSuite) TestTaskNack_Reschedule() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	s.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now()))

	executable.Nack(consts.ErrTaskRetry) // this error won't trigger re-submit
	s.Equal(ctasks.TaskStatePending, executable.State())
}

func (s *executableSuite) TestTaskCancellation() {
	executable := s.newTestExecutable(func(_ tasks.Task) bool {
		return true
	})

	executable.Cancel()

	s.NoError(executable.Execute()) // should be no-op and won't invoke executor

	executable.Ack() // should be no-op
	s.Equal(ctasks.TaskStateCancelled, executable.State())

	executable.Nack(errors.New("some random error")) // should be no-op and won't invoke scheduler or rescheduler

	executable.Reschedule() // should be no-op and won't invoke rescheduler

	// all error should be treated as non-retryable to break retry loop
	s.False(executable.IsRetryableError(errors.New("some random error")))
}

func (s *executableSuite) newTestExecutable(
	filter TaskFilter,
) Executable {
	return NewExecutable(
		tasks.NewFakeTask(
			definition.NewWorkflowKey(
				tests.NamespaceID.String(),
				tests.WorkflowID,
				tests.RunID,
			),
			tasks.CategoryTransfer,
			s.timeSource.Now(),
		),
		filter,
		s.mockExecutor,
		s.mockScheduler,
		s.mockRescheduler,
		s.timeSource,
		log.NewTestLogger(),
		dynamicconfig.GetIntPropertyFn(100),
		QueueTypeActiveTransfer,
		dynamicconfig.GetDurationPropertyFn(namespaceCacheRefreshInterval),
	)
}
