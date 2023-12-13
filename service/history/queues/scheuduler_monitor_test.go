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
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tests"
)

var (
	testSchedulerMonitorOptions = schedulerMonitorOptions{
		aggregationCount:    10,
		aggregationDuration: 200 * time.Millisecond,
	}
)

type (
	schedulerMonitorSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockNamespaceRegistry *namespace.MockRegistry
		mockMetricsHandler    *metrics.MockHandler
		mockTimerMetric       *metrics.MockTimerIface
		mockTimeSource        *clock.EventTimeSource

		schedulerMonitor *schedulerMonitor
	}
)

func TestSchedulerMonitorSuite(t *testing.T) {
	s := new(schedulerMonitorSuite)
	suite.Run(t, s)
}

func (s *schedulerMonitorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockMetricsHandler = metrics.NewMockHandler(s.controller)
	s.mockTimerMetric = metrics.NewMockTimerIface(s.controller)
	s.mockTimeSource = clock.NewEventTimeSource()

	s.mockNamespaceRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockMetricsHandler.EXPECT().WithTags(gomock.Any()).Return(s.mockMetricsHandler).AnyTimes()
	s.mockMetricsHandler.EXPECT().Timer(metrics.QueueScheduleLatency.Name()).Return(s.mockTimerMetric).AnyTimes()

	s.schedulerMonitor = newSchedulerMonitor(
		func(e Executable) TaskChannelKey {
			return TaskChannelKey{
				NamespaceID: tests.NamespaceID.String(),
				Priority:    tasks.PriorityHigh,
			}
		},
		s.mockNamespaceRegistry,
		s.mockTimeSource,
		s.mockMetricsHandler,
		testSchedulerMonitorOptions,
	)
}

func (s *schedulerMonitorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *schedulerMonitorSuite) TestRecordStart_AggregationCount() {
	s.schedulerMonitor.Start()
	defer s.schedulerMonitor.Stop()

	now := clock.NewRealTimeSource().Now()
	scheduledTime := now
	singleScheduleLatency := time.Millisecond * 10
	s.mockTimeSource.Update(now)

	totalExecutables := 2*testSchedulerMonitorOptions.aggregationCount + 10

	s.mockTimerMetric.EXPECT().Record(
		time.Duration(testSchedulerMonitorOptions.aggregationCount) * singleScheduleLatency,
	).Times(totalExecutables / testSchedulerMonitorOptions.aggregationCount)

	for numExecutables := 0; numExecutables != totalExecutables; numExecutables++ {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetScheduledTime().Return(scheduledTime).Times(1)

		now = now.Add(singleScheduleLatency)
		s.mockTimeSource.Update(now)
		s.schedulerMonitor.RecordStart(mockExecutable)
	}
}

func (s *schedulerMonitorSuite) TestRecordStart_AggregationDuration() {
	s.schedulerMonitor.Start()
	defer s.schedulerMonitor.Stop()

	now := clock.NewRealTimeSource().Now()
	singleScheduleLatency := time.Millisecond * 10
	s.mockTimeSource.Update(now)

	totalExecutables := testSchedulerMonitorOptions.aggregationCount / 2

	done := make(chan struct{})
	s.mockTimerMetric.EXPECT().Record(
		// although # of executable is less than aggregationCount
		// the value emitted should be scaled
		time.Duration(testSchedulerMonitorOptions.aggregationCount) * singleScheduleLatency,
	).Do(func(time.Duration, ...metrics.Tag) {
		close(done)
	}).Times(1)

	for numExecutables := 0; numExecutables != totalExecutables; numExecutables++ {
		mockExecutable := NewMockExecutable(s.controller)
		mockExecutable.EXPECT().GetScheduledTime().Return(now).Times(1)

		now = now.Add(singleScheduleLatency)
		s.mockTimeSource.Update(now)
		s.schedulerMonitor.RecordStart(mockExecutable)

		// simulate the case where task submission is very low frequency
		now = now.Add(10 * singleScheduleLatency)
	}

	// wait for the metric emission ticker
	select {
	case <-done:
	case <-time.NewTimer(3 * testSchedulerMonitorOptions.aggregationDuration).C:
		s.Fail("metric emission ticker should fire earlier")
	}
}
