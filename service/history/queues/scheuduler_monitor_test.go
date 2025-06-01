package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
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
