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
