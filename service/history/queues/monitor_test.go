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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

type (
	monitorSuite struct {
		suite.Suite
		*require.Assertions

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

	s.monitor = newMonitor(tasks.CategoryTypeScheduled,
		&MonitorOptions{
			ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
			SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
		},
	)
	s.alertCh = s.monitor.AlertCh()
}

func (s *monitorSuite) TearDownTest() {
	s.monitor.Close()
}

func (s *monitorSuite) TestReaderWatermarkStats() {
	_, ok := s.monitor.GetReaderWatermark(defaultReaderId)
	s.False(ok)

	now := time.Now().Truncate(monitorWatermarkPrecision)
	s.monitor.SetReaderWatermark(defaultReaderId, tasks.NewKey(now, rand.Int63()))
	watermark, ok := s.monitor.GetReaderWatermark(defaultReaderId)
	s.True(ok)
	s.Equal(tasks.NewKey(
		now.Truncate(monitorWatermarkPrecision),
		0,
	), watermark)

	for i := 0; i != s.monitor.options.ReaderStuckCriticalAttempts(); i++ {
		now = now.Add(time.Millisecond * 100)
		s.monitor.SetReaderWatermark(defaultReaderId, tasks.NewKey(now, rand.Int63()))
	}

	alert := <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeReaderStuck,
		AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
			ReaderID: defaultReaderId,
			CurrentWatermark: tasks.NewKey(
				now.Truncate(monitorWatermarkPrecision),
				0,
			),
		},
	}, *alert)
}

func (s *monitorSuite) TestSliceCount() {
	s.Equal(0, s.monitor.GetTotalSliceCount())
	s.Equal(0, s.monitor.GetSliceCount(defaultReaderId))

	threshold := s.monitor.options.SliceCountCriticalThreshold()
	s.monitor.SetSliceCount(defaultReaderId, threshold/2)
	s.Equal(threshold/2, s.monitor.GetTotalSliceCount())
	select {
	case <-s.alertCh:
		s.Fail("should not trigger alert")
	default:
	}

	s.monitor.SetSliceCount(defaultReaderId, threshold*2)
	s.Equal(threshold*2, s.monitor.GetTotalSliceCount())
	alert := <-s.alertCh
	s.Equal(Alert{
		AlertType: AlertTypeSliceCount,
		AlertAttributesSliceCount: &AlertAttributesSlicesCount{
			CurrentSliceCount:  threshold * 2,
			CriticalSliceCount: threshold,
		},
	}, *alert)

	s.monitor.SetSliceCount(defaultReaderId+1, 1)
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
