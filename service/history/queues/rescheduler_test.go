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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
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

	s.timeSource = clock.NewEventTimeSource()

	s.rescheduler = NewRescheduler(
		s.mockScheduler,
		s.timeSource,
		metrics.NoopScope,
	)
}

func (s *rescheudulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *rescheudulerSuite) TestReschedule_NoRescheduleLimit() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable/2; i++ {
		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds())),
		)
		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			rescheduleInterval+time.Duration(rand.Int63n(time.Minute.Nanoseconds())),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).Times(numExecutable / 2)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.Reschedule(0)
	s.Equal(numExecutable/2, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_WithRescheduleLimit() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable; i++ {
		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds())),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	rescheduleSize := 3

	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Return(true, nil).Times(rescheduleSize)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.Reschedule(rescheduleSize)
	s.Equal(numExecutable-rescheduleSize, s.rescheduler.Len())
}

func (s *rescheudulerSuite) TestReschedule_SubmissionFailed() {
	now := time.Now()
	s.timeSource.Update(now)
	rescheduleInterval := time.Minute

	numExecutable := 10
	for i := 0; i != numExecutable; i++ {
		s.rescheduler.Add(
			NewMockExecutable(s.controller),
			time.Duration(rand.Int63n(rescheduleInterval.Nanoseconds())),
		)
	}
	s.Equal(numExecutable, s.rescheduler.Len())

	numSubmitted := 0
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) (bool, error) {
		submitted := rand.Intn(2) == 0
		if submitted {
			numSubmitted++
		}
		return submitted, nil
	}).Times(numExecutable)

	s.timeSource.Update(now.Add(rescheduleInterval))
	s.rescheduler.Reschedule(0)
	s.Equal(numExecutable-numSubmitted, s.rescheduler.Len())
}
