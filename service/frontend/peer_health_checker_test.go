package frontend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func testPeerHosts(prefix string, n int) []membership.HostInfo {
	hosts := make([]membership.HostInfo, n)
	for i := range hosts {
		hosts[i] = membership.NewHostInfoFromAddress(fmt.Sprintf("%s-%d:7234", prefix, i))
	}
	return hosts
}

func testPeerHealthCheckConfig(pct, threshold float64, probeTimeout time.Duration) peerHealthCheckConfig {
	return peerHealthCheckConfig{
		Enabled: dynamicconfig.GetBoolPropertyFn(true),
		Settings: dynamicconfig.GetTypedPropertyFn(dynamicconfig.PeerHealthCheckSettings{
			Interval:         time.Second,
			ProbeTimeout:     probeTimeout,
			SamplePercentage: pct,
			FailureThreshold: threshold,
			MinFailedHosts:   2,
		}),
	}
}

// withPeerSettings returns a copy of cfg with edit applied to its settings.
func withPeerSettings(cfg peerHealthCheckConfig, edit func(*dynamicconfig.PeerHealthCheckSettings)) peerHealthCheckConfig {
	s := cfg.Settings()
	edit(&s)
	cfg.Settings = dynamicconfig.GetTypedPropertyFn(s)
	return cfg
}

// newPeerTestMonitor returns a monitor whose resolvers report the given hosts per role.
func newPeerTestMonitor(t *testing.T, hostsByRole map[primitives.ServiceName][]membership.HostInfo) *membership.MockMonitor {
	ctrl := gomock.NewController(t)
	monitor := membership.NewMockMonitor(ctrl)
	for role, hosts := range hostsByRole {
		resolver := membership.NewMockServiceResolver(ctrl)
		resolver.EXPECT().AvailableMembers().Return(hosts).AnyTimes()
		monitor.EXPECT().GetResolver(role).Return(resolver, nil).AnyTimes()
	}
	return monitor
}

func newPeerTestChecker(
	monitor membership.Monitor,
	probe peerProbeFn,
	config peerHealthCheckConfig,
) *peerHealthChecker {
	return newPeerHealthChecker(
		monitor,
		probe,
		func(healthpb.HealthCheckResponse_ServingStatus) {},
		config,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
}

// timingOutAddresses returns a probe that times out for the given addresses and responds otherwise.
func timingOutAddresses(timingOut ...string) peerProbeFn {
	set := make(map[string]bool, len(timingOut))
	for _, addr := range timingOut {
		set[addr] = true
	}
	return func(_ context.Context, _ primitives.ServiceName, address string) error {
		if set[address] {
			return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
		}
		return nil
	}
}

// probeMetricCounts sums the probe metric recorded for each "target_service/outcome".
func probeMetricCounts(capture *metricstest.Capture) map[string]int64 {
	counts := make(map[string]int64)
	for _, r := range capture.SnapshotMetric(metrics.FrontendPeerHealthCheckProbe.Name()) {
		counts[r.Tags["target_service"]+"/"+r.Tags["outcome"]] += r.Value.(int64)
	}
	return counts
}

func TestClassifyPeerProbeError(t *testing.T) {
	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()
	probeDeadlineCtx, probeDeadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer probeDeadlineCancel()
	probeCanceledCtx, probeCanceledCancel := context.WithCancel(context.Background())
	probeCanceledCancel()
	for _, tc := range []struct {
		name       string
		checkerCtx context.Context
		probeCtx   context.Context
		err        error
		want       peerProbeOutcome
	}{
		{name: "nil", err: nil, want: peerProbeResponse},
		{name: "not found", err: status.Error(codes.NotFound, "x"), want: peerProbeResponse},
		{name: "permission denied", err: status.Error(codes.PermissionDenied, "x"), want: peerProbeResponse},
		{name: "connection refused transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: connect: connection refused"`), want: peerProbeRefused},
		{name: "i/o timeout transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeTimeout},
		{name: "dial context deadline exceeded transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial: context deadline exceeded"`), want: peerProbeTimeout},
		{name: "connect timed out transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: connect: connection timed out"`), want: peerProbeTimeout},
		{name: "operation timed out transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: operation timed out"`), want: peerProbeTimeout},
		{name: "no such host transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: lookup history: no such host"`), want: peerProbeUnresolved},
		{name: "DNS lookup transport timeout", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: lookup history: i/o timeout"`), want: peerProbeTimeout},
		{name: "DNS lookup routing error", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: lookup history: dial udp 10.0.0.2:53: connect: network is unreachable"`), want: peerProbeUnresolved},
		{name: "no route to host transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: no route to host"`), want: peerProbeNetwork},
		{name: "network is unreachable transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: network is unreachable"`), want: peerProbeNetwork},
		{name: "closing transport is not an application response", err: status.Error(codes.Unavailable, "closing transport due to: EOF, received prior goaway: "), want: peerProbeUnknown},
		{name: "unavailable with unknown origin", err: status.Error(codes.Unavailable, "connection reset by peer"), want: peerProbeUnknown},
		{name: "unavailable with timeout wording but unknown origin", err: serviceerror.NewUnavailable("application i/o timeout"), want: peerProbeUnknown},
		{name: "unavailable with network wording but unknown origin", err: serviceerror.NewUnavailable("application network is unreachable"), want: peerProbeUnknown},
		{name: "short TCP dial status", err: status.Error(codes.Unavailable, "dial tcp 10.0.0.1:7234: connect: connection timed out"), want: peerProbeTimeout},
		{name: "short TCP dial service error", err: serviceerror.NewUnavailable("dial tcp 10.0.0.1:7234: connect: connection timed out"), want: peerProbeTimeout},
		{name: "plain i/o timeout error", err: errors.New("dial tcp 10.0.0.1:7234: i/o timeout"), want: peerProbeTimeout},
		{name: "plain unrecognized error", err: errors.New("boom"), want: peerProbeUnknown},
		{name: "wrapped permission denied", err: fmt.Errorf("probe: %w", status.Error(codes.PermissionDenied, "x")), want: peerProbeResponse},
		{name: "permission denied returned before probe deadline", probeCtx: probeDeadlineCtx, err: status.Error(codes.PermissionDenied, "x"), want: peerProbeResponse},
		{name: "service error permission denied", err: serviceerror.NewPermissionDenied("x", ""), want: peerProbeResponse},
		{name: "service error not found", err: serviceerror.NewNotFound("x"), want: peerProbeResponse},
		{name: "service error connection refused", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: connect: connection refused"`), want: peerProbeRefused},
		{name: "service error i/o timeout transport", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeTimeout},
		{name: "service error connection timed out transport", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: connect: connection timed out"`), want: peerProbeTimeout},
		{name: "service error no route to host transport", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: no route to host"`), want: peerProbeNetwork},
		{name: "service error operation timed out transport", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: operation timed out"`), want: peerProbeTimeout},
		{name: "remote service error deadline exceeded", err: serviceerror.NewDeadlineExceeded("remote deadline"), want: peerProbeResponse},
		{name: "service error deadline exceeded after checker cancellation", checkerCtx: doneCtx, err: serviceerror.NewDeadlineExceeded("context deadline exceeded"), want: peerProbeUnknown},
		{name: "converted closing client connection", err: serviceerror.FromStatus(status.Convert(status.Error(codes.Canceled, "grpc: the client connection is closing"))), want: peerProbeUnknown},
		{name: "local probe canceled while returning transport error", probeCtx: probeCanceledCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp: i/o timeout"`), want: peerProbeUnknown},
		{name: "wrapped service error permission denied", err: fmt.Errorf("wrap: %w", serviceerror.NewPermissionDenied("x", "")), want: peerProbeResponse},
		{name: "remote deadline exceeded status", err: status.Error(codes.DeadlineExceeded, "x"), want: peerProbeResponse},
		{name: "local probe deadline", probeCtx: probeDeadlineCtx, err: context.DeadlineExceeded, want: peerProbeTimeout},
		{name: "deadline exceeded after checker cancellation", checkerCtx: doneCtx, err: status.Error(codes.DeadlineExceeded, "x"), want: peerProbeUnknown},
		{name: "raw deadline exceeded after checker cancellation", checkerCtx: doneCtx, err: context.DeadlineExceeded, want: peerProbeUnknown},
		{name: "i/o timeout after checker cancellation", checkerCtx: doneCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeUnknown},
		{name: "dial context deadline exceeded after checker cancellation", checkerCtx: doneCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial: context deadline exceeded"`), want: peerProbeUnknown},
		{name: "service error i/o timeout after checker cancellation", checkerCtx: doneCtx, err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeUnknown},
		{name: "closing client connection", err: status.Error(codes.Canceled, "grpc: the client connection is closing"), want: peerProbeUnknown},
		{name: "raw canceled", err: context.Canceled, want: peerProbeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkerCtx := tc.checkerCtx
			if checkerCtx == nil {
				checkerCtx = context.Background()
			}
			probeCtx := tc.probeCtx
			if probeCtx == nil {
				probeCtx = checkerCtx
			}
			require.Equal(t, tc.want, classifyPeerProbeError(checkerCtx, probeCtx, tc.err))
		})
	}
}

func TestRunRound_DefaultSamplingDetectsPartialFailure(t *testing.T) {
	t.Parallel()

	for _, hostCount := range []int{3, 4} {
		t.Run(fmt.Sprintf("%d hosts", hostCount), func(t *testing.T) {
			t.Parallel()

			monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
				primitives.HistoryService:  testPeerHosts("history", hostCount),
				primitives.MatchingService: testPeerHosts("matching", 1),
			})
			var historyCalls atomic.Int32
			probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
				if role == primitives.HistoryService && historyCalls.Add(1) <= 2 {
					return context.DeadlineExceeded
				}
				return nil
			}
			cfg := peerHealthCheckConfig{Settings: dynamicconfig.GetTypedPropertyFn(dynamicconfig.DefaultPeerHealthCheckSettings)}
			checker := newPeerTestChecker(monitor, probe, cfg)

			require.Equal(t, peerRoundFail, checker.runRound(t.Context()))
			require.Equal(t, int32(3), historyCalls.Load())
		})
	}
}

func TestRunRound_UsesSameMinFailedHostsForSamplingAndEvaluation(t *testing.T) {
	t.Parallel()

	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 1),
	})
	var minFailedHosts, historyCalls atomic.Int32
	minFailedHosts.Store(2)
	cfg := peerHealthCheckConfig{Settings: func() dynamicconfig.PeerHealthCheckSettings {
		settings := dynamicconfig.DefaultPeerHealthCheckSettings
		settings.MinFailedHosts = int(minFailedHosts.Load())
		return settings
	}}
	probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
		if role == primitives.HistoryService && historyCalls.Add(1) <= 2 {
			minFailedHosts.Store(3)
			return context.DeadlineExceeded
		}
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, cfg)

	require.Equal(t, peerRoundFail, checker.runRound(t.Context()))
	require.Equal(t, int32(3), historyCalls.Swap(0))
	require.Equal(t, peerRoundPass, checker.runRound(t.Context()))
	require.Equal(t, int32(4), historyCalls.Load())
}

func TestSamplePeerHosts(t *testing.T) {
	for _, tc := range []struct {
		name           string
		n              int
		pct            float64
		minFailedHosts int
		want           int
	}{
		{name: "empty", n: 0, pct: 0.5, minFailedHosts: 2, want: 0},
		{name: "single host", n: 1, pct: 0.5, minFailedHosts: 2, want: 1},
		{name: "room for a reachable host", n: 10, pct: 0.2, minFailedHosts: 3, want: 4},
		{name: "half rounds up", n: 11, pct: 0.5, minFailedHosts: 2, want: 6},
		{name: "hundred hosts half", n: 100, pct: 0.5, minFailedHosts: 2, want: 50},
		{name: "full percentage", n: 100, pct: 1, minFailedHosts: 2, want: 100},
		{name: "minimum capped by available hosts", n: 10, pct: 0.5, minFailedHosts: math.MaxInt, want: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := testPeerHosts("host", tc.n)
			got := samplePeerHosts(hosts, tc.pct, tc.minFailedHosts)
			require.Len(t, got, tc.want)

			seen := make(map[string]struct{}, len(got))
			for _, h := range got {
				_, dup := seen[h.GetAddress()]
				require.False(t, dup, "duplicate address %s", h.GetAddress())
				seen[h.GetAddress()] = struct{}{}
			}
		})
	}

	t.Run("does not mutate input", func(t *testing.T) {
		hosts := testPeerHosts("host", 50)
		before := make([]string, len(hosts))
		for i, h := range hosts {
			before[i] = h.GetAddress()
		}
		_ = samplePeerHosts(hosts, 0.5, 2)
		for i, h := range hosts {
			require.Equal(t, before[i], h.GetAddress())
		}
	})
}

func TestPeerHealthCheckConfigFallbacks(t *testing.T) {
	bad := testPeerHealthCheckConfig(-1, 2, 0)
	c := newPeerTestChecker(nil, nil, bad)
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.ProbeTimeout, c.probeTimeout())
	require.InDelta(t, dynamicconfig.DefaultPeerHealthCheckSettings.SamplePercentage, c.samplePercentage(), 1e-9)
	require.InDelta(t, dynamicconfig.DefaultPeerHealthCheckSettings.FailureThreshold, c.failureThreshold(), 1e-9)
	bad = withPeerSettings(bad, func(s *dynamicconfig.PeerHealthCheckSettings) { s.MinFailedHosts = 0 })
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.MinFailedHosts, newPeerTestChecker(nil, nil, bad).minFailedHosts())

	good := testPeerHealthCheckConfig(0.8, 1, 15*time.Second)
	c = newPeerTestChecker(nil, nil, good)
	require.Equal(t, 15*time.Second, c.probeTimeout())
	require.InDelta(t, 0.8, c.samplePercentage(), 1e-9)
	require.InDelta(t, 1.0, c.failureThreshold(), 1e-9)
	require.Equal(t, 2, c.minFailedHosts())
	good = withPeerSettings(good, func(s *dynamicconfig.PeerHealthCheckSettings) { s.MinFailedHosts = 3 })
	require.Equal(t, 3, newPeerTestChecker(nil, nil, good).minFailedHosts())
}

func TestRunRound_AllHealthy(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	calls := map[primitives.ServiceName]*atomic.Int32{
		primitives.HistoryService:  {},
		primitives.MatchingService: {},
	}
	probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
		calls[role].Add(1)
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(0.5, 0.5, time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
	require.Equal(t, int32(3), calls[primitives.HistoryService].Load())
	require.Equal(t, int32(3), calls[primitives.MatchingService].Load())
}

func TestRunRound_TimeoutsAtThresholdFailPerRole(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	// History times out 2/4 = 50%, at the threshold. Combined across roles it would be 2/8 and pass.
	probe := timingOutAddresses("history-0:7234", "history-1:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
}

func TestRunRound_MixedTimeoutAndNetworkFailuresFail(t *testing.T) {
	t.Parallel()

	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	probe := func(_ context.Context, role primitives.ServiceName, address string) error {
		if role == primitives.HistoryService {
			switch address {
			case "history-0:7234":
				return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
			case "history-1:7234":
				return serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: network is unreachable"`)
			}
		}
		return nil
	}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(monitor, probe, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 0.5, time.Second), handler, log.NewNoopLogger())

	require.Equal(t, peerRoundFail, checker.runRound(t.Context()))
	require.Equal(t, map[string]int64{
		"history/response":  2,
		"history/timeout":   1,
		"history/network":   1,
		"matching/response": 2,
	}, probeMetricCounts(capture))
}

func TestRunRound_AllNetworkFailuresAreDownstreamOutage(t *testing.T) {
	t.Parallel()

	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
		if role == primitives.HistoryService {
			return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: no route to host"`)
		}
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(t.Context()))
}

func TestRunRound_BelowThresholdPasses(t *testing.T) {
	history := testPeerHosts("history", 20)
	timingOut := make([]string, 0, 9)
	for _, h := range history[:9] {
		timingOut = append(timingOut, h.GetAddress())
	}
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  history,
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	checker := newPeerTestChecker(monitor, timingOutAddresses(timingOut...), testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
}

func TestRunRound_MinFailedHostsPreventsFailure(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 2),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	// One of two probes times out, reaching the 50% threshold but falling short of the two-timeout minimum.
	var historyCalls atomic.Int32
	probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
		if role == primitives.HistoryService && historyCalls.Add(1) == 1 {
			return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
		}
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(0.5, 0.5, time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
	require.Equal(t, int32(2), historyCalls.Load())
}

func TestRemoteDeadlineExceededCountsAsResponse(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(nil, func(context.Context, primitives.ServiceName, string) error {
		return serviceerror.NewDeadlineExceeded("remote deadline")
	}, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 0.5, time.Second), handler, log.NewNoopLogger())

	result := checker.runProbe(context.Background(), peerProbeTarget{role: primitives.HistoryService, address: "history:7234"})
	results := make(chan peerProbeResult, 1)
	results <- result
	counts, _ := checker.collectProbeResults(context.Background(), map[primitives.ServiceName]int{primitives.HistoryService: 1}, results)

	require.Equal(t, 1, counts[primitives.HistoryService][peerProbeResponse])
	require.Equal(t, map[string]int64{"history/response": 1}, probeMetricCounts(capture))
}

func TestRunRound_AllSampledTimeOutIsOutage(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	probe := func(context.Context, primitives.ServiceName, string) error {
		return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
}

func TestRunRound_EmptyRoleIsIgnored(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: nil,
	})
	// History is still evaluated: 2/4 time out.
	probe := timingOutAddresses("history-0:7234", "history-1:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
}

func TestRunRound_MissingRoleWithHealthyOtherRoleIsUnknown(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  nil,
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	checker := newPeerTestChecker(monitor, timingOutAddresses(), testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundUnknown, checker.runRound(context.Background()))
}

func TestRunRound_ResolverErrorIsIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	monitor := membership.NewMockMonitor(ctrl)
	historyResolver := membership.NewMockServiceResolver(ctrl)
	historyResolver.EXPECT().AvailableMembers().Return(testPeerHosts("history", 4)).AnyTimes()
	monitor.EXPECT().GetResolver(primitives.HistoryService).Return(historyResolver, nil).AnyTimes()
	monitor.EXPECT().GetResolver(primitives.MatchingService).Return(nil, errors.New("resolver unavailable")).AnyTimes()
	probe := timingOutAddresses("history-0:7234", "history-1:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
}

func TestRunRound_QueuedProbesHaveOwnDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const probeTimeout = 10 * time.Second
		const failedHistoryProbes = peerHealthCheckMaxConcurrentProbes + 1
		healthyHistory := fmt.Sprintf("history-%d:7234", failedHistoryProbes)
		monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
			primitives.HistoryService:  testPeerHosts("history", failedHistoryProbes+1),
			primitives.MatchingService: testPeerHosts("matching", 1),
		})
		probe := func(ctx context.Context, role primitives.ServiceName, address string) error {
			if role == primitives.MatchingService || address == healthyHistory {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
		handler := metricstest.NewCaptureHandler()
		capture := handler.StartCapture()
		checker := newPeerHealthChecker(monitor, probe, func(healthpb.HealthCheckResponse_ServingStatus) {},
			testPeerHealthCheckConfig(1, 0.5, probeTimeout), handler, log.NewNoopLogger())

		start := time.Now()
		roundCtx, cancel := context.WithTimeout(t.Context(), 5*probeTimeout)
		defer cancel()
		require.Equal(t, peerRoundFail, checker.runRound(roundCtx))
		require.Equal(t, 2*probeTimeout, time.Since(start))
		require.Equal(t, map[string]int64{
			"history/response":  1,
			"history/timeout":   failedHistoryProbes,
			"matching/response": 1,
		}, probeMetricCounts(capture))
	})
}

func TestEvaluateRound_UnknownOutcomesPreventDownstreamOutagePass(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 0.5, time.Second))
	planned := map[primitives.ServiceName]int{primitives.HistoryService: 2, primitives.MatchingService: 2}
	counts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeUnknown: 1, peerProbeTimeout: 1},
		primitives.MatchingService: {peerProbeUnresolved: 2},
	}

	require.Equal(t, peerRoundUnknown, checker.evaluateRound(planned, counts, nil, 2))
	completedOutageCounts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeTimeout: 2},
		primitives.MatchingService: {peerProbeUnresolved: 2},
	}
	require.Equal(t, peerRoundPass, checker.evaluateRound(planned, completedOutageCounts, nil, 2))
}

func TestEvaluateRoundUnknownProbesCanChangeVerdict(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 0.5, time.Second))
	planned := map[primitives.ServiceName]int{primitives.HistoryService: 4, primitives.MatchingService: 1}
	counts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeResponse: 1, peerProbeTimeout: 1, peerProbeUnknown: 2},
		primitives.MatchingService: {peerProbeResponse: 1},
	}
	require.Equal(t, peerRoundUnknown, checker.evaluateRound(planned, counts, nil, 2))
	counts[primitives.HistoryService] = map[peerProbeOutcome]int{peerProbeResponse: 3, peerProbeUnknown: 1}
	require.Equal(t, peerRoundPass, checker.evaluateRound(planned, counts, nil, 2))
}

func TestCollectProbeResultsKeepsRepresentativeFailureError(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 0.5, time.Second))
	permissionErr := serviceerror.NewPermissionDenied("application response", "")
	for _, outcome := range []peerProbeOutcome{peerProbeTimeout, peerProbeNetwork} {
		t.Run(string(outcome), func(t *testing.T) {
			failureErr := errors.New("probe failed")
			results := make(chan peerProbeResult, 2)
			results <- peerProbeResult{role: primitives.HistoryService, outcome: peerProbeResponse, err: permissionErr}
			results <- peerProbeResult{role: primitives.HistoryService, outcome: outcome, err: failureErr}

			_, representativeErrors := checker.collectProbeResults(t.Context(), map[primitives.ServiceName]int{primitives.HistoryService: 2}, results)

			require.Same(t, failureErr, representativeErrors[primitives.HistoryService])
		})
	}
}

func TestRunRound_ConcurrencyCapIsShared(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()

		// Keep the first round's probes active after cancellation to exercise the shared cap.
		const hostsPerRole = peerHealthCheckMaxConcurrentProbes/2 + 1
		entered := make(chan struct{}, 4*hostsPerRole)
		probe := func(context.Context, primitives.ServiceName, string) error {
			entered <- struct{}{}
			<-gate
			return nil
		}
		monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
			primitives.HistoryService:  testPeerHosts("history", hostsPerRole),
			primitives.MatchingService: testPeerHosts("matching", hostsPerRole),
		})
		checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 0.5, time.Minute))
		firstCtx, cancelFirstRound := context.WithCancel(ctx)
		defer cancelFirstRound()
		firstDone := make(chan string, 1)
		go func() { firstDone <- checker.runRound(firstCtx) }()
		for range peerHealthCheckMaxConcurrentProbes {
			select {
			case <-entered:
			case verdict := <-firstDone:
				t.Fatalf("round ended before filling the shared probe cap: %s", verdict)
			case <-ctx.Done():
				t.Fatal("timed out waiting for the round to fill the shared probe cap")
			}
		}
		synctest.Wait()
		require.Empty(t, entered, "probes must stop starting once all slots are in use")

		cancelFirstRound()
		select {
		case verdict := <-firstDone:
			require.Equal(t, peerRoundUnknown, verdict)
		case <-ctx.Done():
			t.Fatal("first round did not exit after cancellation")
		}

		secondDone := make(chan string, 1)
		go func() { secondDone <- checker.runRound(ctx) }()
		synctest.Wait()
		require.Empty(t, entered, "the second round must wait for earlier probes to release their slots")
		require.Empty(t, secondDone)

		release()
		select {
		case verdict := <-secondDone:
			require.Equal(t, peerRoundPass, verdict)
		case <-ctx.Done():
			t.Fatal("second round did not finish after the slots were released")
		}
		synctest.Wait()
		require.Len(t, entered, 2*hostsPerRole, "only the second round may start its queued probes")
	})
}

// statusRecorder captures every setStatus call in order.
type statusRecorder struct {
	mu       sync.Mutex
	statuses []healthpb.HealthCheckResponse_ServingStatus
}

func (r *statusRecorder) set(s healthpb.HealthCheckResponse_ServingStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses = append(r.statuses, s)
}

func (r *statusRecorder) list() []healthpb.HealthCheckResponse_ServingStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.statuses)
}

func (r *statusRecorder) last() (healthpb.HealthCheckResponse_ServingStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.statuses) == 0 {
		return 0, false
	}
	return r.statuses[len(r.statuses)-1], true
}

// newPeerLoopChecker builds a checker whose flag and probe outcome can be toggled by the test.
func newPeerLoopChecker(
	t *testing.T,
	enabled *atomic.Bool,
	healthy *atomic.Bool,
	probeCalls *atomic.Int32,
	interval time.Duration,
	rec *statusRecorder,
) *peerHealthChecker {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	// An unhealthy probe times out for half of History's hosts, which fails the round.
	probe := func(_ context.Context, _ primitives.ServiceName, address string) error {
		probeCalls.Add(1)
		if !healthy.Load() && (address == "history-0:7234" || address == "history-1:7234") {
			return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
		}
		return nil
	}
	cfg := testPeerHealthCheckConfig(1, 0.5, time.Second)
	cfg.Enabled = enabled.Load
	cfg = withPeerSettings(cfg, func(s *dynamicconfig.PeerHealthCheckSettings) { s.Interval = interval })
	return newPeerHealthChecker(monitor, probe, rec.set, cfg, metrics.NoopMetricsHandler, log.NewNoopLogger())
}

func TestTick_FailedRoundThenPass(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, time.Second, rec)

	checker.tick(context.Background())
	healthy.Store(true)
	checker.tick(context.Background())

	require.Equal(t, []healthpb.HealthCheckResponse_ServingStatus{
		healthpb.HealthCheckResponse_NOT_SERVING,
		healthpb.HealthCheckResponse_SERVING,
	}, rec.list())
}

func TestTick_FlagTurnedOffWhileNotServing(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, time.Second, rec)

	checker.tick(context.Background())
	require.Equal(t, []healthpb.HealthCheckResponse_ServingStatus{healthpb.HealthCheckResponse_NOT_SERVING}, rec.list())

	enabled.Store(false)
	callsBefore := probeCalls.Load()
	checker.tick(context.Background())

	require.Equal(t, callsBefore, probeCalls.Load())
	last, ok := rec.last()
	require.True(t, ok)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, last)
}

func TestTick_UnknownRoundKeepsStatus(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  nil,
		primitives.MatchingService: nil,
	})
	rec := &statusRecorder{}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(monitor, timingOutAddresses(), rec.set,
		testPeerHealthCheckConfig(1, 0.5, time.Second), handler, log.NewNoopLogger())

	checker.tick(context.Background())

	require.Empty(t, rec.list())
	rounds := capture.SnapshotMetric(metrics.FrontendPeerHealthCheckRound.Name())
	require.Len(t, rounds, 1)
	require.Equal(t, peerRoundUnknown, rounds[0].Tags["outcome"])
}

func TestStartStop_LoopRunsAndStops(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const interval = time.Second
		var enabled, healthy atomic.Bool
		enabled.Store(true)
		healthy.Store(true)
		var probeCalls atomic.Int32
		rec := &statusRecorder{}
		checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, interval, rec)

		checker.Start()
		t.Cleanup(checker.Stop)
		synctest.Wait()
		require.Equal(t, []healthpb.HealthCheckResponse_ServingStatus{healthpb.HealthCheckResponse_SERVING}, rec.list())
		firstRoundCalls := probeCalls.Load()
		require.Positive(t, firstRoundCalls)

		<-time.After(interval)
		synctest.Wait()
		require.Equal(t, []healthpb.HealthCheckResponse_ServingStatus{
			healthpb.HealthCheckResponse_SERVING,
			healthpb.HealthCheckResponse_SERVING,
		}, rec.list())
		require.Equal(t, 2*firstRoundCalls, probeCalls.Load())

		checker.Stop()
		checker.Stop() // second call must be a no-op
		stoppedStatuses := rec.list()
		stoppedProbeCalls := probeCalls.Load()
		<-time.After(interval)
		synctest.Wait()
		require.Equal(t, stoppedStatuses, rec.list())
		require.Equal(t, stoppedProbeCalls, probeCalls.Load())
	})
}

func TestStop_InterruptsInFlightRound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		rec := &statusRecorder{}
		started := make(chan struct{}, peerHealthCheckMaxConcurrentProbes+1)
		monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
			primitives.HistoryService:  testPeerHosts("history", peerHealthCheckMaxConcurrentProbes+1),
			primitives.MatchingService: testPeerHosts("matching", 1),
		})
		// Let one probe report before cancellation so the round has a partial result to discard.
		var responded atomic.Bool
		probe := func(ctx context.Context, _ primitives.ServiceName, _ string) error {
			if responded.CompareAndSwap(false, true) {
				return nil
			}
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		handler := metricstest.NewCaptureHandler()
		capture := handler.StartCapture()
		checker := newPeerHealthChecker(monitor, probe, rec.set,
			testPeerHealthCheckConfig(1, 0.5, time.Minute), handler, log.NewNoopLogger())
		checker.Start()
		defer checker.Stop()
		defer checker.cancel() // Unblock the loop if Stop fails to cancel it.
		for range peerHealthCheckMaxConcurrentProbes {
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("timed out waiting for the round to fill the probe cap")
			}
		}
		stopped := make(chan struct{})
		go func() {
			checker.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatal("checker did not stop promptly")
		}
		synctest.Wait()

		require.Empty(t, started, "queued probes must not start after shutdown")
		require.Empty(t, rec.list(), "an interrupted round must not set status")
		require.Empty(t, probeMetricCounts(capture), "an interrupted round must not emit probe metrics")
		require.Empty(t, capture.SnapshotMetric(metrics.FrontendPeerHealthCheckRound.Name()), "an interrupted round must not emit a round metric")
	})
}

func TestStart_ZeroIntervalDoesNotPanic(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	healthy.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, 0, rec)

	require.NotPanics(t, func() {
		checker.Start()
		checker.Stop()
	})
}

func TestPeerHealthCheckIntervalZeroFallsBack(t *testing.T) {
	cfg := withPeerSettings(testPeerHealthCheckConfig(1, 0.5, time.Second), func(s *dynamicconfig.PeerHealthCheckSettings) {
		s.Interval = 0
	})
	require.Equal(t, minPeerHealthCheckInterval, newPeerTestChecker(nil, nil, cfg).interval())
}

func TestStop_BeforeStartPreventsStart(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	healthy.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, time.Hour, rec)

	checker.Stop()
	checker.Start()

	require.Empty(t, rec.list())
	require.Equal(t, int32(0), probeCalls.Load())
}
