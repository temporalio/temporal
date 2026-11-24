package queues

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/tasks"
)

var monitorTestFireTime = time.Unix(1000, 0).UTC()

type (
	monitorSuite struct {
		suite.Suite
		*require.Assertions

		mockTimeSource *clock.EventTimeSource

		monitor *monitorImpl
		alertCh <-chan *Alert
	}
)

func TestMonitorSuite(t *testing.T) {
	s := new(monitorSuite)
	suite.Run(t, s)
}

func (s *monitorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.mockTimeSource = clock.NewEventTimeSource()
	s.monitor = newMonitor(
		tasks.CategoryTypeScheduled,
		s.mockTimeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		&MonitorOptions{
			PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
			ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
			ReaderStuckShadowMode:       dynamicconfig.GetBoolPropertyFn(false),
			SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
		},
	)
	s.alertCh = s.monitor.AlertCh()
}

func (s *monitorSuite) TearDownTest() {
	s.monitor.Close()
}

func (s *monitorSuite) TestPendingTasksStats() {
	s.Equal(0, s.monitor.GetTotalPendingTaskCount())
	s.Equal(0, s.monitor.GetSlicePendingTaskCount(&SliceImpl{}))

	threshold := s.monitor.options.PendingTasksCriticalCount()

	slice1 := &SliceImpl{}
	s.monitor.SetSlicePendingTaskCount(slice1, threshold/2)
	s.Equal(threshold/2, s.monitor.GetSlicePendingTaskCount(slice1))
	select {
	case <-s.alertCh:
		s.Fail("should not trigger alert")
	default:
	}

	s.monitor.SetSlicePendingTaskCount(slice1, threshold*2)
	s.Equal(threshold*2, s.monitor.GetTotalPendingTaskCount())
	alert := <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   threshold * 2,
			CiriticalPendingTaskCount: threshold,
		},
	}, *alert)

	slice2 := &SliceImpl{}
	s.monitor.SetSlicePendingTaskCount(slice2, 1)
	select {
	case <-s.alertCh:
		s.Fail("should have only one outstanding pending task alert")
	default:
	}

	s.monitor.ResolveAlert(alert.AlertType)
	s.monitor.SetSlicePendingTaskCount(slice2, 1)
	s.Equal(threshold*2+1, s.monitor.GetTotalPendingTaskCount())
	alert = <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   threshold*2 + 1,
			CiriticalPendingTaskCount: threshold,
		},
	}, *alert)

	s.monitor.RemoveSlice(slice1)
	s.Equal(1, s.monitor.GetTotalPendingTaskCount())
}

func (s *monitorSuite) TestReaderWatermarkStats() {
	_, ok := s.monitor.GetReaderWatermark(DefaultReaderId)
	s.False(ok)

	now := time.Now().Truncate(monitorWatermarkPrecision)
	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()), true)
	watermark, ok := s.monitor.GetReaderWatermark(DefaultReaderId)
	s.True(ok)
	s.Equal(tasks.NewKey(
		now.Truncate(monitorWatermarkPrecision),
		0,
	), watermark)

	for i := 0; i != s.monitor.options.ReaderStuckCriticalAttempts(); i++ {
		now = now.Add(time.Millisecond * 100)
		s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()), true)
	}

	alert := <-s.alertCh
	expectedAlert := Alert{
		AlertType: AlertTypeReaderStuck,
		AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
			ReaderID: DefaultReaderId,
			CurrentWatermark: tasks.NewKey(
				now.Truncate(monitorWatermarkPrecision),
				0,
			),
		},
	}
	s.Equal(expectedAlert, *alert)

	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()), true)
	select {
	case <-s.alertCh:
		s.Fail("should have only one outstanding slice count alert")
	default:
	}

	s.monitor.ResolveAlert(alert.AlertType)
	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()), true)
	alert = <-s.alertCh
	s.Equal(expectedAlert, *alert)
}

func (s *monitorSuite) receivedStuckAlert() bool {
	select {
	case alert := <-s.alertCh:
		s.Equal(AlertTypeReaderStuck, alert.AlertType)
		return true
	default:
		return false
	}
}

func (s *monitorSuite) TestReaderWatermarkStats_DrainedReadClearsAttempts() {
	key := tasks.NewKey(monitorTestFireTime, 1)
	criticalAttempts := s.monitor.options.ReaderStuckCriticalAttempts()

	for i := 0; i != criticalAttempts-1; i++ {
		s.monitor.SetReaderWatermark(DefaultReaderId, key, true)
	}

	s.monitor.SetReaderWatermark(DefaultReaderId, key, false)
	s.False(s.receivedStuckAlert(), "a drained read must not count toward the threshold")

	for i := 0; i != criticalAttempts-1; i++ {
		s.monitor.SetReaderWatermark(DefaultReaderId, key, true)
	}
	s.False(s.receivedStuckAlert(), "a drained read must clear what earlier reads accumulated")

	s.monitor.SetReaderWatermark(DefaultReaderId, key, true)
	s.True(s.receivedStuckAlert(), "consecutive reads that leave tasks behind must still alert")
}

func (s *monitorSuite) TestReaderWatermarkStats_DrainedReadRecordsItsWatermark() {
	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), false)

	watermark, ok := s.monitor.GetReaderWatermark(DefaultReaderId)
	s.True(ok)
	s.Equal(tasks.NewKey(monitorTestFireTime, 0), watermark)
}

func (s *monitorSuite) TestReaderWatermarkStats_AlertsOnFirstCutShortReadWhenThresholdIsOne() {
	s.monitor.options.ReaderStuckCriticalAttempts = dynamicconfig.GetIntPropertyFn(1)

	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)

	s.True(s.receivedStuckAlert(), "a threshold of one should alert on the first read that left tasks behind")
}

func (s *monitorSuite) TestReaderWatermarkStats_AdvancingWatermarkResetsAttempts() {
	criticalAttempts := s.monitor.options.ReaderStuckCriticalAttempts()
	inWindow := tasks.NewKey(monitorTestFireTime, 1)
	inNextWindow := tasks.NewKey(monitorTestFireTime.Add(monitorWatermarkPrecision), 1)

	for i := 0; i != criticalAttempts-1; i++ {
		s.monitor.SetReaderWatermark(DefaultReaderId, inWindow, true)
	}
	for i := 0; i != criticalAttempts-1; i++ {
		s.monitor.SetReaderWatermark(DefaultReaderId, inNextWindow, true)
	}

	s.False(s.receivedStuckAlert(), "attempts from earlier windows must not carry into the current one")
}

func (s *monitorSuite) TestReaderWatermarkStats_ZeroThresholdDisablesTheAlert() {
	s.monitor.options.ReaderStuckCriticalAttempts = dynamicconfig.GetIntPropertyFn(0)

	for i := 0; i != 10; i++ {
		s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)
	}

	s.False(s.receivedStuckAlert(), "a threshold of zero must disable the alert")
}

func (s *monitorSuite) TestReaderWatermarkStats_ShadowModeReportsWithoutAlerting() {
	options := *s.monitor.options
	options.ReaderStuckShadowMode = dynamicconfig.GetBoolPropertyFn(true)

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	monitor := newMonitor(tasks.CategoryTypeScheduled, s.mockTimeSource, log.NewTestLogger(), metricsHandler, &options)
	defer monitor.Close()

	start := s.mockTimeSource.Now()
	for i := 0; i != options.ReaderStuckCriticalAttempts()*2; i++ {
		monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)
	}

	select {
	case <-monitor.AlertCh():
		s.Fail("shadow mode must not raise the alert")
	default:
	}

	// Silenced after the first report, so a run of reads past the threshold reports once.
	records := capture.Snapshot()["queue_alert_shadow"]
	s.Len(records, 1)
	s.Equal(readerStuckActionName, records[0].Tags[metrics.QueueActionTagName])

	s.mockTimeSource.Update(start.Add(defaultAlertSilenceDuration + time.Second))
	monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)
	s.Len(capture.Snapshot()["queue_alert_shadow"], 2, "a reader still stuck must report again once the silence expires")
}

func (s *monitorSuite) TestReaderWatermarkStats_ShadowModeStopsReportingAfterClose() {
	options := *s.monitor.options
	options.ReaderStuckShadowMode = dynamicconfig.GetBoolPropertyFn(true)

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	monitor := newMonitor(tasks.CategoryTypeScheduled, s.mockTimeSource, log.NewTestLogger(), metricsHandler, &options)
	start := s.mockTimeSource.Now()
	for i := 0; i != options.ReaderStuckCriticalAttempts(); i++ {
		monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)
	}
	s.Len(capture.Snapshot()["queue_alert_shadow"], 1)

	monitor.Close()
	s.mockTimeSource.Update(start.Add(defaultAlertSilenceDuration + time.Second))
	monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(monitorTestFireTime, 1), true)

	s.Len(capture.Snapshot()["queue_alert_shadow"], 1, "a closed monitor must not keep reporting")
}

func (s *monitorSuite) TestSliceCount() {
	s.Equal(0, s.monitor.GetTotalSliceCount())
	s.Equal(0, s.monitor.GetSliceCount(DefaultReaderId))

	threshold := s.monitor.options.SliceCountCriticalThreshold()
	s.monitor.SetSliceCount(DefaultReaderId, threshold/2)
	s.Equal(threshold/2, s.monitor.GetTotalSliceCount())
	select {
	case <-s.alertCh:
		s.Fail("should not trigger alert")
	default:
	}

	s.monitor.SetSliceCount(DefaultReaderId, threshold*2)
	s.Equal(threshold*2, s.monitor.GetTotalSliceCount())
	alert := <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeSliceCount,
		AlertAttributesSliceCount: &AlertAttributesSlicesCount{
			CurrentSliceCount:  threshold * 2,
			CriticalSliceCount: threshold,
		},
	}, *alert)

	s.monitor.SetSliceCount(DefaultReaderId+1, 1)
	select {
	case <-s.alertCh:
		s.Fail("should have only one outstanding slice count alert")
	default:
	}

	s.monitor.ResolveAlert(alert.AlertType)
	s.monitor.SetSliceCount(DefaultReaderId+1, 1)
	s.Equal(threshold*2+1, s.monitor.GetTotalSliceCount())
	alert = <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeSliceCount,
		AlertAttributesSliceCount: &AlertAttributesSlicesCount{
			CurrentSliceCount:  threshold*2 + 1,
			CriticalSliceCount: threshold,
		},
	}, *alert)
}

func (s *monitorSuite) TestResolveAlert() {
	sliceCount := s.monitor.options.SliceCountCriticalThreshold() * 2

	s.monitor.SetSliceCount(DefaultReaderId, sliceCount) // trigger an alert

	alert := <-s.alertCh
	s.NotNil(alert)
	s.monitor.ResolveAlert(alert.AlertType)

	// alert should be resolved,
	// which means we can trigger the same alert type again
	s.monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case alert := <-s.alertCh:
		s.NotNil(alert)
	default:
		s.FailNow("Can't trigger new alert, previous alert likely not resolved")
	}
}

func (s *monitorSuite) TestSilenceAlert() {
	now := time.Now()
	s.mockTimeSource.Update(now)

	sliceCount := s.monitor.options.SliceCountCriticalThreshold() * 2
	s.monitor.SetSliceCount(DefaultReaderId, sliceCount) // trigger an alert

	alert := <-s.alertCh
	s.NotNil(alert)
	s.monitor.SilenceAlert(alert.AlertType)

	// alert should be silenced,
	// which means we can't trigger the same alert type again
	s.monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case <-s.alertCh:
		s.FailNow("Alert not silenced")
	default:
	}

	// other alert types should still be able to fire
	pendingTaskCount := s.monitor.options.PendingTasksCriticalCount() * 2
	s.monitor.SetSlicePendingTaskCount(&SliceImpl{}, pendingTaskCount)
	select {
	case alert := <-s.alertCh:
		s.NotNil(alert)
	default:
		s.FailNow("Alerts with a different type should still be able to fire")
	}

	now = now.Add(defaultAlertSilenceDuration * 2)
	s.mockTimeSource.Update(now)

	// same alert should be able to fire after the silence duration
	s.monitor.SetSliceCount(DefaultReaderId, sliceCount)
	select {
	case alert := <-s.alertCh:
		s.NotNil(alert)
	default:
		s.FailNow("Same alert type should fire after silence duration")
	}
}
