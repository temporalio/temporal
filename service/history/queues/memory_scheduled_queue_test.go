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
	"sync/atomic"
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
	memoryScheduledQueueSuite struct {
		suite.Suite
		*require.Assertions

		controller     *gomock.Controller
		mockTimeSource *clock.EventTimeSource
		mockScheduler  *ctasks.MockScheduler[ctasks.Task]

		scheduledQueue *memoryScheduledQueue
	}
)

func TestMemoryScheduledQueueSuite(t *testing.T) {
	s := new(memoryScheduledQueueSuite)
	suite.Run(t, s)
}

func (s *memoryScheduledQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTimeSource = clock.NewEventTimeSource()
	s.mockTimeSource.Update(time.Now().UTC())

	s.mockScheduler = ctasks.NewMockScheduler[ctasks.Task](s.controller)
	s.mockScheduler.EXPECT().Start().AnyTimes()
	s.mockScheduler.EXPECT().Stop().AnyTimes()

	s.scheduledQueue = newMemoryScheduledQueue(
		s.mockScheduler,
		s.mockTimeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	s.scheduledQueue.Start()
}

func (s *memoryScheduledQueueSuite) TearDownTest() {
	s.scheduledQueue.Stop()
}

func (s *memoryScheduledQueueSuite) Test_ThreeInOrderTasks() {

	now := s.mockTimeSource.Now()
	t1 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	calls := atomic.Int32{}
	calls.Store(3)
	gomock.InOrder(
		s.mockScheduler.EXPECT().TrySubmit(t2).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		s.mockScheduler.EXPECT().TrySubmit(t1).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
		s.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) }),
	)

	s.scheduledQueue.Add(t1)
	s.scheduledQueue.Add(t2)
	s.scheduledQueue.Add(t3)

	// To ensure all timers have fired.
	s.Eventually(func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) Test_ThreeCancelledTasks() {

	s.scheduledQueue.scheduler = s.mockScheduler

	now := s.mockTimeSource.Now()
	t1 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(20 * time.Millisecond))
	t2 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(10 * time.Millisecond))
	t3 := s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(30 * time.Millisecond))

	t1.Cancel()
	t2.Cancel()

	calls := atomic.Int32{}
	calls.Store(1)
	s.mockScheduler.EXPECT().TrySubmit(t3).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })

	s.scheduledQueue.Add(t1)
	s.scheduledQueue.Add(t2)
	s.scheduledQueue.Add(t3)

	s.Eventually(func() bool { return calls.Load() == 0 }, time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) Test_1KRandomTasks() {

	s.scheduledQueue.scheduler = s.mockScheduler

	now := s.mockTimeSource.Now()
	t := make([]*speculativeWorkflowTaskTimeoutExecutable, 1000)
	calls := atomic.Int32{}

	for i := 0; i < 1000; i++ {
		t[i] = s.newSpeculativeWorkflowTaskTimeoutTestExecutable(now.Add(time.Duration(rand.Intn(100)) * time.Microsecond))

		// Randomly cancel some tasks.
		if rand.Intn(2) == 0 {
			t[i].Cancel()
		} else {
			calls.Add(1)
		}
	}

	for i := 0; i < 1000; i++ {
		if t[i].State() != ctasks.TaskStateCancelled {
			s.mockScheduler.EXPECT().TrySubmit(t[i]).Return(true).Do(func(_ ctasks.Task) { calls.Add(-1) })
		}
	}

	for i := 0; i < 1000; i++ {
		s.scheduledQueue.Add(t[i])
	}

	// To ensure all timers have fired.
	s.Eventually(func() bool { return calls.Load() == 0 }, 10*time.Second, 100*time.Millisecond)
}

func (s *memoryScheduledQueueSuite) newSpeculativeWorkflowTaskTimeoutTestExecutable(
	visibilityTimestamp time.Time,
) *speculativeWorkflowTaskTimeoutExecutable {

	wttt := &tasks.WorkflowTaskTimeoutTask{
		VisibilityTimestamp: visibilityTimestamp,
	}

	return newSpeculativeWorkflowTaskTimeoutExecutable(
		NewExecutable(
			0,
			wttt,
			nil,
			nil,
			nil,
			NewNoopPriorityAssigner(),
			s.mockTimeSource,
			nil,
			nil,
			nil,
			nil,
		),
		wttt,
	)
}
