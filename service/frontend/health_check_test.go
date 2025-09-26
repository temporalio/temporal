package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

type (
	healthCheckerSuite struct {
		suite.Suite
		controller *gomock.Controller

		membershipMonitor *membership.MockMonitor
		resolver          *membership.MockServiceResolver

		checker *healthCheckerImpl
	}
)

func TestHealthCheckerSuite(t *testing.T) {
	s := new(healthCheckerSuite)
	suite.Run(t, s)
}

func (s *healthCheckerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.membershipMonitor = membership.NewMockMonitor(s.controller)
	s.resolver = membership.NewMockServiceResolver(s.controller)
	s.membershipMonitor.EXPECT().GetResolver(gomock.Any()).Return(s.resolver, nil).AnyTimes()

	checker := NewHealthChecker(
		primitives.HistoryService,
		s.membershipMonitor,
		func() float64 {
			return 0.25
		},
		func() float64 {
			return 0.15
		},
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			switch hostAddress {
			case "1", "3":
				return enumsspb.HEALTH_STATE_SERVING, nil
			case "2":
				return enumsspb.HEALTH_STATE_UNSPECIFIED, errors.New("test")
			case "4":
				return enumsspb.HEALTH_STATE_DECLINED_SERVING, nil
			default:
				return enumsspb.HEALTH_STATE_NOT_SERVING, nil
			}
		},
		log.NewNoopLogger(),
	)
	healthChecker, ok := checker.(*healthCheckerImpl)
	if !ok {
		s.Fail("The constructor did not return correct type")
	}
	s.checker = healthChecker
}

func (s *healthCheckerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *healthCheckerSuite) Test_Check_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("1"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_Not_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("3"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("5"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_Declined_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("4"),
		membership.NewHostInfoFromAddress("7"),
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_DECLINED_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_No_Available_Hosts() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_GetResolver_Error() {
	// Create a new checker for this test to avoid conflicting expectations
	membershipMonitor := membership.NewMockMonitor(s.controller)
	membershipMonitor.EXPECT().GetResolver(primitives.HistoryService).Return(nil, errors.New("resolver error"))

	checker := NewHealthChecker(
		primitives.HistoryService,
		membershipMonitor,
		func() float64 { return 0.25 },
		func() float64 { return 0.15 },
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			return enumsspb.HEALTH_STATE_SERVING, nil
		},
		log.NewNoopLogger(),
	)

	state, err := checker.Check(context.Background())
	s.Error(err)
	s.Equal(enumsspb.HEALTH_STATE_UNSPECIFIED, state)
	s.Contains(err.Error(), "resolver error")
}

func (s *healthCheckerSuite) Test_Check_Boundary_Failure_Percentage_Equals_Threshold() {
	// Test when failure percentage exactly equals the threshold (0.25)
	// With 4 hosts, 1 failed = 0.25 (25%), should return SERVING since it's not > threshold
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"), // SERVING
		membership.NewHostInfoFromAddress("2"), // UNSPECIFIED (failed)
		membership.NewHostInfoFromAddress("3"), // SERVING
		membership.NewHostInfoFromAddress("1"), // SERVING
	})

	state, err := s.checker.Check(context.Background())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, state)
}

func (s *healthCheckerSuite) Test_Check_Single_Host_Scenarios() {
	testCases := []struct {
		name          string
		hostAddress   string
		expectedState enumsspb.HealthState
	}{
		{
			name:          "single host serving",
			hostAddress:   "1", // SERVING
			expectedState: enumsspb.HEALTH_STATE_SERVING,
		},
		{
			name:          "single host failed",
			hostAddress:   "2", // UNSPECIFIED (failed)
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			name:          "single host declined serving",
			hostAddress:   "4",                               // DECLINED_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING, // Combined logic: 0% failed + 100% declined = 100% > 25% threshold
		},
		{
			name:          "single host not serving",
			hostAddress:   "5", // NOT_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
				membership.NewHostInfoFromAddress(tc.hostAddress),
			})

			state, err := s.checker.Check(context.Background())
			s.NoError(err)
			s.Equal(tc.expectedState, state)
		})
	}
}

func (s *healthCheckerSuite) Test_Check_Context_Cancellation() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("1"),
		membership.NewHostInfoFromAddress("2"),
	})

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Create a checker with a health check function that respects context cancellation
	checker := NewHealthChecker(
		primitives.HistoryService,
		s.membershipMonitor,
		func() float64 { return 0.25 },
		func() float64 { return 0.15 },
		func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error) {
			select {
			case <-ctx.Done():
				return enumsspb.HEALTH_STATE_UNSPECIFIED, ctx.Err()
			default:
				return enumsspb.HEALTH_STATE_SERVING, nil
			}
		},
		log.NewNoopLogger(),
	)

	state, err := checker.Check(ctx)
	s.NoError(err)                                    // Context cancellation in individual health checks should not fail the overall check
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, state) // All hosts will return UNSPECIFIED due to cancellation
}

func (s *healthCheckerSuite) Test_Check_Mixed_Host_States_Edge_Cases() {
	testCases := []struct {
		name          string
		hosts         []string
		expectedState enumsspb.HealthState
		description   string
	}{
		{
			name:          "edge case: 50% declined serving equals minimum threshold",
			hosts:         []string{"4", "4", "1", "1"},      // 2 declined, 2 serving out of 4
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING, // Combined: 0% failed + 50% declined = 50% > 25% threshold
			description:   "50% declined serving triggers combined failure threshold, returns NOT_SERVING",
		},
		{
			name:          "edge case: 60% declined serving exceeds minimum threshold",
			hosts:         []string{"4", "4", "4", "1", "1"},      // 3 declined, 2 serving out of 5
			expectedState: enumsspb.HEALTH_STATE_DECLINED_SERVING, // 60% > 40% minimum threshold
			description:   "60% declined serving should trigger DECLINED_SERVING response",
		},
		{
			name:          "edge case: mixed failures just under threshold",
			hosts:         []string{"2", "1", "1", "1", "1"}, // 1 failed (20%), 4 serving (80%) out of 5
			expectedState: enumsspb.HEALTH_STATE_SERVING,     // 20% < 25% threshold
			description:   "20% failures should still return SERVING",
		},
		{
			name:          "edge case: combined failures and declined just over threshold",
			hosts:         []string{"2", "4", "1", "1"}, // 1 failed (25%) + 1 declined (25%) = 50% > 25% threshold
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
			description:   "Combined 50% failures and declined serving should trigger NOT_SERVING",
		},
		{
			name:          "edge case: all declined serving with many hosts",
			hosts:         []string{"4", "4", "4", "4", "4"}, // All declined serving
			expectedState: enumsspb.HEALTH_STATE_DECLINED_SERVING,
			description:   "100% declined serving should return DECLINED_SERVING",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			hostInfos := make([]membership.HostInfo, len(tc.hosts))
			for i, host := range tc.hosts {
				hostInfos[i] = membership.NewHostInfoFromAddress(host)
			}
			s.resolver.EXPECT().AvailableMembers().Return(hostInfos)

			state, err := s.checker.Check(context.Background())
			s.NoError(err, tc.description)
			s.Equal(tc.expectedState, state, tc.description)
		})
	}
}

func (s *healthCheckerSuite) Test_GetProportionOfNotReadyHosts() {
	testCases := []struct {
		name                             string
		proportionOfDeclinedServingHosts float64
		totalHosts                       int
		expectedProportion               float64
	}{
		{
			name:                             "zero proportion",
			proportionOfDeclinedServingHosts: 0.0,
			totalHosts:                       10,
			expectedProportion:               0.2,
		},
		{
			name:                             "small proportion with few hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       10,
			expectedProportion:               0.2, // 2/10 = 0.2 since numHostsToFail < 2
		},
		{
			name:                             "small proportion with many hosts",
			proportionOfDeclinedServingHosts: 0.1,
			totalHosts:                       100,
			expectedProportion:               0.1, // 10 hosts > 2, so use original proportion
		},
		{
			name:                             "large proportion",
			proportionOfDeclinedServingHosts: 0.8,
			totalHosts:                       10,
			expectedProportion:               0.8, // 8 hosts > 2, so use original proportion
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			proportion := ensureMinimumProportionOfHosts(tc.proportionOfDeclinedServingHosts, tc.totalHosts)
			s.Equal(tc.expectedProportion, proportion)
		})
	}
}
