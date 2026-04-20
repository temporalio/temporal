package frontend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/health"
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
		func() time.Duration {
			return 0 * time.Second
		},
		func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error) {
			if strings.HasPrefix(hostAddress, "serving") {
				return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_SERVING}, nil
			} else if strings.HasPrefix(hostAddress, "error") {
				return nil, errors.New("test")
			} else if strings.HasPrefix(hostAddress, "declined") {
				return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_DECLINED_SERVING}, nil
			}
			return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_NOT_SERVING}, nil
		},
		log.NewNoopLogger(),
	)
	s.checker = checker.(*healthCheckerImpl)
}

func (s *healthCheckerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *healthCheckerSuite) Test_Unique_Host_Health() {
	s.checker.hostFailureTimeThreshold = func() time.Duration {
		return 1 * time.Second
	}
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
	}).Times(4)
	result, err := s.checker.Check(context.Background(), time.Unix(1, 0))
	s.Require().NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, result.State)
	// Make sure a consistent unhealthy host keeps the state in NOT_SERVING
	for i := range 3 {
		result, err = s.checker.Check(context.Background(), time.Unix(int64(i)+2, 0))
		s.Require().NoError(err)
		s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, result.State)
	}
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorC"),
	})
	result, err = s.checker.Check(context.Background(), time.Unix(3, 0))
	s.Require().NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, result.State)
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorD"),
	})
	result, err = s.checker.Check(context.Background(), time.Unix(4, 0))
	s.Require().NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, result.State)
}

func (s *healthCheckerSuite) Test_Unhealthy_Host_Tracker() {
	tracker := &unhealthyHostTracker{hosts: make(map[string]unhealthyHostRecord)}
	tracker.trackUnhealthyHost("A", time.Unix(1, 0), enumsspb.HEALTH_STATE_NOT_SERVING)
	tracker.trackUnhealthyHost("B", time.Unix(1, 0), enumsspb.HEALTH_STATE_DECLINED_SERVING)
	tracker.trackUnhealthyHost("C", time.Unix(1, 0), enumsspb.HEALTH_STATE_INTERNAL_ERROR)
	declinedServing, otherUnhealthy := tracker.unhealthyHosts(time.Unix(2, 0), 1*time.Second)
	s.Require().Len(declinedServing, 1, "Should be one declined host. Instead found: %+v", declinedServing)
	s.Require().Len(otherUnhealthy, 2, "Should be two other unhealthy hosts. Instead found: %+v", otherUnhealthy)
	declinedServing, otherUnhealthy = tracker.unhealthyHosts(time.Unix(1, 0), 1*time.Second)
	s.Require().Len(declinedServing, 0, "Should be no declined hosts. Instead found: %+v", declinedServing)
	s.Require().Len(otherUnhealthy, 0, "Should be no other unhealthy hosts. Instead found: %+v", otherUnhealthy)

	// Demonstrate status updates to existing host
	tracker.trackUnhealthyHost("A", time.Unix(2, 0), enumsspb.HEALTH_STATE_NOT_SERVING)
	s.Require().Equal(tracker.hosts["A"].failedAt, time.Unix(1, 0), "Host A should keep original time, but was %v", tracker.hosts["A"].failedAt.Unix())
	tracker.trackUnhealthyHost("A", time.Unix(3, 0), enumsspb.HEALTH_STATE_DECLINED_SERVING)
	s.Require().Equal(tracker.hosts["A"].lastSeenHealth, enumsspb.HEALTH_STATE_DECLINED_SERVING, "Host A should now be DECLINED_SERVING, but was %s", tracker.hosts["A"].lastSeenHealth.String())
	s.Require().Equal(tracker.hosts["A"].failedAt, time.Unix(1, 0), "Host A should keep original time, but was %v", tracker.hosts["A"].failedAt.Unix())

	// Test stale entries
	tracker.clearStaleEntries([]*healthspb.HostHealthDetail{
		{Address: "A", State: enumsspb.HEALTH_STATE_DECLINED_SERVING},
		{Address: "B", State: enumsspb.HEALTH_STATE_DECLINED_SERVING},
		{Address: "C", State: enumsspb.HEALTH_STATE_INTERNAL_ERROR},
	})
	declinedServing, otherUnhealthy = tracker.unhealthyHosts(time.Unix(2, 0), 1*time.Second)
	// No change
	s.Require().Len(declinedServing, 2, "Should be two declined hosts. Instead found: %+v", declinedServing)
	s.Require().Len(otherUnhealthy, 1, "Should be one other unhealthy host. Instead found: %+v", otherUnhealthy)
	tracker.clearStaleEntries([]*healthspb.HostHealthDetail{
		{Address: "A", State: enumsspb.HEALTH_STATE_DECLINED_SERVING},
		{Address: "B", State: enumsspb.HEALTH_STATE_SERVING},
		{Address: "C", State: enumsspb.HEALTH_STATE_INTERNAL_ERROR},
	})
	declinedServing, otherUnhealthy = tracker.unhealthyHosts(time.Unix(2, 0), 1*time.Second)
	// Declined host is healthy now!
	s.Require().Len(declinedServing, 1, "Should be one declined host. Instead found: %+v", declinedServing)
	s.Require().Len(otherUnhealthy, 1, "Should be one other unhealthy host. Instead found: %+v", otherUnhealthy)

	tracker.clearStaleEntries([]*healthspb.HostHealthDetail{})
	// Host list empty -> No more unhealthy hosts
	declinedServing, otherUnhealthy = tracker.unhealthyHosts(time.Unix(2, 0), 1*time.Second)
	s.Require().Len(declinedServing, 0, "Should be no declined hosts. Instead found: %+v", declinedServing)
	s.Require().Len(otherUnhealthy, 0, "Should be no other unhealthy hosts. Instead found: %+v", otherUnhealthy)
}

func (s *healthCheckerSuite) Test_Check_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
		membership.NewHostInfoFromAddress("servingC"),
		membership.NewHostInfoFromAddress("servingD"),
	})

	result, err := s.checker.Check(context.Background(), time.Now())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_SERVING, result.State)
}

func (s *healthCheckerSuite) Test_Check_Not_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
		membership.NewHostInfoFromAddress("servingC"),
		membership.NewHostInfoFromAddress("declinedD"),
		membership.NewHostInfoFromAddress("E"), // not-serving
	})

	result, err := s.checker.Check(context.Background(), time.Now())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, result.State)
}

func (s *healthCheckerSuite) Test_Check_Declined_Serving() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
		membership.NewHostInfoFromAddress("declinedC"),
		membership.NewHostInfoFromAddress("declinedD"),
		membership.NewHostInfoFromAddress("declinedE"),
		membership.NewHostInfoFromAddress("declinedF"),
		membership.NewHostInfoFromAddress("G"),
	})

	result, err := s.checker.Check(context.Background(), time.Now())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_DECLINED_SERVING, result.State)
}

func (s *healthCheckerSuite) Test_Check_No_Available_Hosts() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{})

	result, err := s.checker.Check(context.Background(), time.Now())
	s.NoError(err)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, result.State)
	s.NotNil(result.ServiceDetail)
	s.Equal("no available hosts in membership", result.ServiceDetail.Message)
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
		func() time.Duration {
			return 0 * time.Second
		},
		func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error) {
			return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_SERVING}, nil
		},
		log.NewNoopLogger(),
	)

	result, err := checker.Check(context.Background(), time.Now())
	s.Error(err)
	s.Equal(enumsspb.HEALTH_STATE_INTERNAL_ERROR, result.State)
	s.Contains(err.Error(), "resolver error")
	s.NotNil(result.ServiceDetail)
	s.Equal(enumsspb.HEALTH_STATE_INTERNAL_ERROR, result.ServiceDetail.State)
	s.Contains(result.ServiceDetail.Message, "failed to get membership resolver")
}

func (s *healthCheckerSuite) Test_Check_Single_Host_Scenarios() {
	testCases := []struct {
		name          string
		hostAddress   string
		expectedState enumsspb.HealthState
	}{
		{
			name:          "single host serving",
			hostAddress:   "servingA", // SERVING
			expectedState: enumsspb.HEALTH_STATE_SERVING,
		},
		{
			name:          "single host failed",
			hostAddress:   "errorA", // NOT_SERVING (failed)
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
		{
			name:          "single host declined serving",
			hostAddress:   "declinedA",                       // DECLINED_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING, // Combined logic: 0% failed + 100% declined = 100% > 25% threshold
		},
		{
			name:          "single host not serving",
			hostAddress:   "A", // NOT_SERVING
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
				membership.NewHostInfoFromAddress(tc.hostAddress),
			})

			result, err := s.checker.Check(context.Background(), time.Now())
			s.NoError(err)
			s.Equal(tc.expectedState, result.State)
		})
	}
}

func (s *healthCheckerSuite) Test_Check_Context_Cancellation() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
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
		func() time.Duration {
			return 0 * time.Second
		},
		func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &historyservice.DeepHealthCheckResponse{State: enumsspb.HEALTH_STATE_SERVING}, nil
			}
		},
		log.NewNoopLogger(),
	)

	result, err := checker.Check(ctx, time.Now())
	s.Require().NoError(err)                                 // Context cancellation in individual health checks should not fail the overall check
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, result.State) // All hosts will return NOT_SERVING due to cancellation
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
			hosts:         []string{"declinedA", "declinedB", "servingC", "servingD"}, // 2 declined, 2 serving out of 4
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,                          // Combined: 0% failed + 50% declined = 50% > 25% threshold
			description:   "50% declined serving triggers combined failure threshold, returns NOT_SERVING",
		},
		{
			name:          "edge case: 60% declined serving exceeds minimum threshold",
			hosts:         []string{"declinedA", "declinedB", "declinedC", "servingD", "servingE"}, // 3 declined, 2 serving out of 5
			expectedState: enumsspb.HEALTH_STATE_DECLINED_SERVING,                                  // 60% > 40% minimum threshold
			description:   "60% declined serving should trigger DECLINED_SERVING response",
		},
		{
			name:          "edge case: mixed failures just under threshold",
			hosts:         []string{"errorA", "servingB", "servingC", "servingD", "servingE"}, // 1 failed (20%), 4 serving (80%) out of 5
			expectedState: enumsspb.HEALTH_STATE_SERVING,                                      // 20% < 25% threshold
			description:   "20% failures should still return SERVING",
		},
		{
			name:          "edge case: combined failures and declined just over threshold",
			hosts:         []string{"errorA", "declinedB", "servingC", "servingD"}, // 1 failed (25%) + 1 declined (25%) = 50% > 25% threshold
			expectedState: enumsspb.HEALTH_STATE_NOT_SERVING,
			description:   "Combined 50% failures and declined serving should trigger NOT_SERVING",
		},
		{
			name:          "edge case: all declined serving with many hosts",
			hosts:         []string{"declinedA", "declinedB", "declinedC", "declinedD", "declinedE"}, // All declined serving
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

			result, err := s.checker.Check(context.Background(), time.Now())
			s.NoError(err, tc.description)
			s.Equal(tc.expectedState, result.State, tc.description)
		})
	}
}

func (s *healthCheckerSuite) Test_Check_ServiceDetail_Populated() {
	s.resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("servingA"),
		membership.NewHostInfoFromAddress("errorB"),
	})

	result, err := s.checker.Check(context.Background(), time.Now())
	s.Require().NoError(err)
	s.NotNil(result.ServiceDetail)
	s.Equal("history", result.ServiceDetail.Service)
	s.Len(result.ServiceDetail.Hosts, 2)
}

func (s *healthCheckerSuite) Test_Check_HostChecks_Propagated() {
	// Create a checker that returns checks in the response
	membershipMonitor := membership.NewMockMonitor(s.controller)
	resolver := membership.NewMockServiceResolver(s.controller)
	membershipMonitor.EXPECT().GetResolver(gomock.Any()).Return(resolver, nil)
	resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("host1"),
	})

	checks := []*healthspb.HealthCheck{
		{
			CheckType: health.CheckTypeRPCLatency,
			State:     enumsspb.HEALTH_STATE_NOT_SERVING,
			Value:     850.0,
			Threshold: 500.0,
			Message:   "RPC latency 850.00ms exceeded 500.00ms threshold",
		},
	}

	checker := NewHealthChecker(
		primitives.HistoryService,
		membershipMonitor,
		func() float64 { return 0.25 },
		func() float64 { return 0.15 },
		func() time.Duration {
			return 0 * time.Second
		},
		func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error) {
			return &historyservice.DeepHealthCheckResponse{
				State:  enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: checks,
			}, nil
		},
		log.NewNoopLogger(),
	)

	result, err := checker.Check(context.Background(), time.Now())
	s.Require().NoError(err)
	s.NotNil(result.ServiceDetail)
	s.Require().Len(result.ServiceDetail.Hosts, 1)
	host := result.ServiceDetail.Hosts[0]
	s.Equal("host1", host.Address)
	s.Equal(enumsspb.HEALTH_STATE_NOT_SERVING, host.State)
	s.Require().Len(host.Checks, 1)
	s.Equal(health.CheckTypeRPCLatency, host.Checks[0].CheckType)
	s.InDelta(850.0, host.Checks[0].Value, 0.01)
	s.InDelta(500.0, host.Checks[0].Threshold, 0.01)
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
