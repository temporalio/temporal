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

package metrics

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	otStopwatchSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller
	}
)

func TestOTStopwatchSuite(t *testing.T) {
	s := new(otStopwatchSuite)
	suite.Run(t, s)
}

func (s *otStopwatchSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *otStopwatchSuite) TearDownTest() {}

func (s *otStopwatchSuite) TestStopwatchReportsExpectedValue() {
	now := time.Now()
	mockClock := clockwork.NewFakeClockAt(now)
	duration := time.Duration(1234)
	mockMetric := NewMockopenTelemetryStopwatchMetric(s.controller)
	metricsMeta := []openTelemetryStopwatchMetric{mockMetric}
	mockMetric.EXPECT().Record(gomock.Any(), duration)
	testObject := newOpenTelemetryStopwatchCustomTimer(metricsMeta, mockClock)
	mockClock.Advance(duration)
	testObject.Stop()
	s.Assert()
}

func (s *otStopwatchSuite) TestStopwatchReportsToAllTimers() {
	now := time.Now()
	mockClock := clockwork.NewFakeClockAt(now)
	duration := time.Duration(1234)
	mockMetric := NewMockopenTelemetryStopwatchMetric(s.controller)
	mockMetric1 := NewMockopenTelemetryStopwatchMetric(s.controller)
	metricsMeta := []openTelemetryStopwatchMetric{mockMetric, mockMetric1}
	mockMetric.EXPECT().Record(gomock.Any(), duration)
	mockMetric1.EXPECT().Record(gomock.Any(), duration)
	testObject := newOpenTelemetryStopwatchCustomTimer(metricsMeta, mockClock)
	mockClock.Advance(duration)
	testObject.Stop()
	s.Assert()
}

func (s *otStopwatchSuite) TestStopwatchSubstractsDurationCorectly() {
	now := time.Now()
	mockClock := clockwork.NewFakeClockAt(now)
	duration := time.Duration(1234)
	expectedDuration := time.Duration(1000)
	mockMetric := NewMockopenTelemetryStopwatchMetric(s.controller)
	mockMetric1 := NewMockopenTelemetryStopwatchMetric(s.controller)
	metricsMeta := []openTelemetryStopwatchMetric{mockMetric, mockMetric1}
	mockMetric.EXPECT().Record(gomock.Any(), expectedDuration)
	mockMetric1.EXPECT().Record(gomock.Any(), expectedDuration)
	testObject := newOpenTelemetryStopwatchCustomTimer(metricsMeta, mockClock)
	mockClock.Advance(duration)
	testObject.Subtract(time.Duration(34))
	testObject.Subtract(time.Duration(200))
	testObject.Stop()
	s.Assert()
}
