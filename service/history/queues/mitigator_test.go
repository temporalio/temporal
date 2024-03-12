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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	mitigatorSuite struct {
		suite.Suite
		*require.Assertions

		mockTimeSource *clock.EventTimeSource

		monitor   *testMonitor
		mitigator *mitigatorImpl
	}

	testMonitor struct {
		Monitor

		resolvedAlertType AlertType
	}
)

func TestMitigatorSuite(t *testing.T) {
	s := new(mitigatorSuite)
	suite.Run(t, s)
}

func (s *mitigatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.mockTimeSource = clock.NewEventTimeSource()
	s.monitor = &testMonitor{}

	// we use a different actionRunner implementation,
	// which doesn't require readerGroup
	s.mitigator = newMitigator(
		nil,
		s.monitor,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		dynamicconfig.GetIntPropertyFn(3),
		GrouperNamespaceID{},
	)
}

func (s *mitigatorSuite) TestMitigate_ActionMatchAlert() {
	testCases := []struct {
		alert          Alert
		expectedAction Action
	}{
		{
			alert: Alert{
				AlertType: AlertTypeQueuePendingTaskCount,
				AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
					CurrentPendingTaskCount:   1000,
					CiriticalPendingTaskCount: 500,
				},
			},
			expectedAction: &actionQueuePendingTask{},
		},
		{
			alert: Alert{
				AlertType: AlertTypeReaderStuck,
				AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
					ReaderID:         1,
					CurrentWatermark: NewRandomKey(),
				},
			},
			expectedAction: &actionReaderStuck{},
		},
		{
			alert: Alert{
				AlertType: AlertTypeSliceCount,
				AlertAttributesSliceCount: &AlertAttributesSlicesCount{
					CurrentSliceCount:  1000,
					CriticalSliceCount: 500,
				},
			},
			expectedAction: &actionSliceCount{},
		},
	}

	var actualAction Action
	s.mitigator.actionRunner = func(
		action Action,
		_ *ReaderGroup,
		_ metrics.Handler,
		_ log.Logger,
	) {
		actualAction = action
	}

	for _, tc := range testCases {
		s.mitigator.Mitigate(tc.alert)
		s.IsType(tc.expectedAction, actualAction)
	}
}

func (s *mitigatorSuite) TestMitigate_ResolveAlert() {
	s.mitigator.actionRunner = func(
		_ Action,
		_ *ReaderGroup,
		_ metrics.Handler,
		_ log.Logger,
	) {
	}

	alert := Alert{
		AlertType: AlertTypeQueuePendingTaskCount,
		AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   1000,
			CiriticalPendingTaskCount: 500,
		},
	}
	s.mitigator.Mitigate(alert)

	s.Equal(alert.AlertType, s.monitor.resolvedAlertType)
}

func (m *testMonitor) ResolveAlert(alertType AlertType) {
	m.resolvedAlertType = alertType
}
