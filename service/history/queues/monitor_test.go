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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

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
		&MonitorOptions{
			PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
			ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
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
	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	watermark, ok := s.monitor.GetReaderWatermark(DefaultReaderId)
	s.True(ok)
	s.Equal(tasks.NewKey(
		now.Truncate(monitorWatermarkPrecision),
		0,
	), watermark)

	for i := 0; i != s.monitor.options.ReaderStuckCriticalAttempts(); i++ {
		now = now.Add(time.Millisecond * 100)
		s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
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

	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	select {
	case <-s.alertCh:
		s.Fail("should have only one outstanding slice count alert")
	default:
	}

	s.monitor.ResolveAlert(alert.AlertType)
	s.monitor.SetReaderWatermark(DefaultReaderId, tasks.NewKey(now, rand.Int63()))
	alert = <-s.alertCh
	s.Equal(expectedAlert, *alert)
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
