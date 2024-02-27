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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	rescheudulerSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		mockScheduler *MockScheduler

		timeSource *clock.EventTimeSource

		rescheduler *reschedulerImpl
	}
)

func TestReschdulerSuite(t *testing.T) {
	s := new(rescheudulerSuite)
	suite.Run(t, s)
}

func (s *rescheudulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockScheduler.EXPECT().TaskChannelKeyFn().Return(
		func(_ Executable) TaskChannelKey { return TaskChannelKey{} },
	).AnyTimes()

	s.timeSource = clock.NewEventTimeSource()

	s.rescheduler = NewRescheduler(
		s.mockScheduler,
		s.timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
}

func (s *rescheudulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *rescheudulerSuite) TestStartStop() {
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		s.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()

	numTasks := 20
	taskCh := make(chan struct{}, numTasks)
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskCh <- struct{}{}
		return true
	}).Times(numTasks)

	for i := 0; i != numTasks; i++ {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
		mockExecutable.EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		rescheduler.Add(
			mockExecutable,
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Millisecond),
		)
	}

	for i := 0; i != numTasks; i++ {
		<-taskCh
	}
	rescheduler.Stop()

	s.Equal(0, rescheduler.Len())
}

func (s *rescheudulerSuite) TestDrain() {
	timeSource := clock.NewRealTimeSource()
	rescheduler := NewRescheduler(
		s.mockScheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	rescheduler.Start()
	rescheduler.Stop()

	for i := 0; i != 10; i++ {
		rescheduler.Add(
			NewMockExecutable(s.controller),
			timeSource.Now().Add(time.Duration(rand.Int63n(300))*time.Second),
		)
	}

	s.Equal(0, rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_NoRescheduleLimit() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable/2; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).Times(1)
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).Times(1)
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)

		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			now.Add(rescheduleInterval+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(numExecutable / 2)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(numExecutable/2, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_TaskChanFull() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).MaxTimes(1)
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	numSubmitted := 0
	numAllowed := numExecutable / 2
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		if numSubmitted < numAllowed {
			numSubmitted++
			return true
		}

		return false
	}).Times(numAllowed + 1)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(numExecutable-numSubmitted, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_DropCancelled() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	for i := 0; i != 10; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStateCancelled).Times(1)
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds()))),
		)
	}

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.reschedule()
	s.Equal(0, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestForceReschedule_ImmediateTask() {
	now := time.Now()
	s.timeSource.Update(now)
	namespaceID := s.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	s.rescheduler.Start()
	defer s.rescheduler.Stop()

	numTask := 10
	taskWG := &sync.WaitGroup{}
	taskWG.Add(numTask)
	for i := 0; i != numTask; i++ {
		mockTask := NewMockExecutable(s.controller)
		mockTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
		mockTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
		mockTask.EXPECT().GetKey().Return(tasks.NewImmediateKey(int64(i))).AnyTimes()
		s.rescheduler.Add(
			mockTask,
			now.Add(time.Minute+time.Duration(rand.Int63n(time.Minute.Nanoseconds()))),
		)
	}

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(numTask)

	s.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	s.Equal(0, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestForceReschedule_ScheduledTask() {
	now := time.Now()
	s.timeSource.Update(now)
	namespaceID := s.mockScheduler.TaskChannelKeyFn()(nil).NamespaceID

	s.rescheduler.Start()
	defer s.rescheduler.Stop()

	taskWG := &sync.WaitGroup{}
	taskWG.Add(1)

	retryingTask := NewMockExecutable(s.controller)
	retryingTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	retryingTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	retryingTask.EXPECT().GetKey().Return(tasks.NewKey(now.Add(-time.Minute), int64(1))).AnyTimes()
	s.rescheduler.Add(
		retryingTask,
		now.Add(time.Minute),
	)

	// schedule queue pre-fetches tasks
	futureTaskTimestamp := now.Add(time.Second)
	futureTask := NewMockExecutable(s.controller)
	futureTask.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	futureTask.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	futureTask.EXPECT().GetKey().Return(tasks.NewKey(futureTaskTimestamp, int64(2))).AnyTimes()
	s.rescheduler.Add(
		futureTask,
		futureTaskTimestamp,
	)

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		taskWG.Done()
		return true
	}).Times(1)

	s.rescheduler.Reschedule(namespaceID)
	taskWG.Wait()
	s.Equal(1, s.rescheduler.Len())
}
