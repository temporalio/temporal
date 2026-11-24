package frontend

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
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

func testPeerHealthCheckConfig(pct float64, maxHosts int, threshold float64, probeTimeout, roundTimeout time.Duration) peerHealthCheckConfig {
	return peerHealthCheckConfig{
		Enabled: dynamicconfig.GetBoolPropertyFn(true),
		Settings: dynamicconfig.GetTypedPropertyFn(dynamicconfig.PeerHealthCheckSettings{
			Interval:         time.Second,
			ProbeTimeout:     probeTimeout,
			RoundTimeout:     roundTimeout,
			SamplePercentage: pct,
			FailureThreshold: threshold,
			MaxHostsPerRole:  maxHosts,
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
		name     string
		roundCtx context.Context
		probeCtx context.Context
		err      error
		want     peerProbeOutcome
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
		{name: "no route to host transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: no route to host"`), want: peerProbeUnresolved},
		{name: "network is unreachable transport", err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: network is unreachable"`), want: peerProbeUnresolved},
		{name: "closing transport is not an application response", err: status.Error(codes.Unavailable, "closing transport due to: EOF, received prior goaway: "), want: peerProbeUnknown},
		{name: "unavailable with unknown origin", err: status.Error(codes.Unavailable, "connection reset by peer"), want: peerProbeUnknown},
		{name: "unavailable with timeout wording but unknown origin", err: serviceerror.NewUnavailable("application i/o timeout"), want: peerProbeUnknown},
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
		{name: "service error operation timed out transport", err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: operation timed out"`), want: peerProbeTimeout},
		{name: "remote service error deadline exceeded", err: serviceerror.NewDeadlineExceeded("remote deadline"), want: peerProbeResponse},
		{name: "service error deadline exceeded after round ended", roundCtx: doneCtx, err: serviceerror.NewDeadlineExceeded("context deadline exceeded"), want: peerProbeUnknown},
		{name: "converted closing client connection", err: serviceerror.FromStatus(status.Convert(status.Error(codes.Canceled, "grpc: the client connection is closing"))), want: peerProbeUnknown},
		{name: "local probe canceled while returning transport error", probeCtx: probeCanceledCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp: i/o timeout"`), want: peerProbeUnknown},
		{name: "wrapped service error permission denied", err: fmt.Errorf("wrap: %w", serviceerror.NewPermissionDenied("x", "")), want: peerProbeResponse},
		{name: "remote deadline exceeded status", err: status.Error(codes.DeadlineExceeded, "x"), want: peerProbeResponse},
		{name: "local probe deadline", probeCtx: probeDeadlineCtx, err: context.DeadlineExceeded, want: peerProbeTimeout},
		{name: "deadline exceeded after round ended", roundCtx: doneCtx, err: status.Error(codes.DeadlineExceeded, "x"), want: peerProbeUnknown},
		{name: "raw deadline exceeded after round ended", roundCtx: doneCtx, err: context.DeadlineExceeded, want: peerProbeUnknown},
		{name: "i/o timeout after round ended", roundCtx: doneCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeUnknown},
		{name: "dial context deadline exceeded after round ended", roundCtx: doneCtx, err: status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial: context deadline exceeded"`), want: peerProbeUnknown},
		{name: "service error i/o timeout after round ended", roundCtx: doneCtx, err: serviceerror.NewUnavailable(`connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`), want: peerProbeUnknown},
		{name: "closing client connection", err: status.Error(codes.Canceled, "grpc: the client connection is closing"), want: peerProbeUnknown},
		{name: "raw canceled", err: context.Canceled, want: peerProbeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roundCtx := tc.roundCtx
			if roundCtx == nil {
				roundCtx = context.Background()
			}
			probeCtx := tc.probeCtx
			if probeCtx == nil {
				probeCtx = roundCtx
			}
			require.Equal(t, tc.want, classifyPeerProbeError(roundCtx, probeCtx, tc.err))
		})
	}
}

func TestSampleProbeTargetsUsesDefaultSamplingForSmallRoles(t *testing.T) {
	for _, hostCount := range []int{3, 4} {
		t.Run(fmt.Sprintf("%d hosts", hostCount), func(t *testing.T) {
			monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
				primitives.HistoryService:  testPeerHosts("history", hostCount),
				primitives.MatchingService: nil,
			})
			cfg := peerHealthCheckConfig{Settings: dynamicconfig.GetTypedPropertyFn(dynamicconfig.DefaultPeerHealthCheckSettings)}
			targets, planned := newPeerTestChecker(monitor, nil, cfg).sampleProbeTargets()

			require.Len(t, targets, 2)
			require.Equal(t, 2, planned[primitives.HistoryService])
		})
	}
}

func TestSamplePeerHosts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		n        int
		pct      float64
		maxHosts int
		want     int
	}{
		{name: "empty", n: 0, pct: 0.5, maxHosts: 20, want: 0},
		{name: "single host rounds up to one", n: 1, pct: 0.5, maxHosts: 20, want: 1},
		{name: "three hosts half rounds up", n: 3, pct: 0.5, maxHosts: 20, want: 2},
		{name: "ten hosts half", n: 10, pct: 0.5, maxHosts: 20, want: 5},
		{name: "hundred hosts capped by max", n: 100, pct: 0.5, maxHosts: 20, want: 20},
		{name: "full percentage capped by max", n: 100, pct: 1, maxHosts: 20, want: 20},
		{name: "negative max", n: 10, pct: 0.5, maxHosts: -1, want: 0},
		{name: "zero max", n: 10, pct: 0.5, maxHosts: 0, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hosts := testPeerHosts("host", tc.n)
			got := samplePeerHosts(hosts, tc.pct, tc.maxHosts)
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
		_ = samplePeerHosts(hosts, 0.5, 20)
		for i, h := range hosts {
			require.Equal(t, before[i], h.GetAddress())
		}
	})
}

func TestPeerHealthCheckConfigFallbacks(t *testing.T) {
	bad := testPeerHealthCheckConfig(-1, -1, 2, 0, -time.Second)
	c := newPeerTestChecker(nil, nil, bad)
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.ProbeTimeout, c.probeTimeout())
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.RoundTimeout, c.roundTimeout())
	require.InDelta(t, dynamicconfig.DefaultPeerHealthCheckSettings.SamplePercentage, c.samplePercentage(), 1e-9)
	require.InDelta(t, dynamicconfig.DefaultPeerHealthCheckSettings.FailureThreshold, c.failureThreshold(), 1e-9)
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.MaxHostsPerRole, c.maxHostsPerRole())
	bad = withPeerSettings(bad, func(s *dynamicconfig.PeerHealthCheckSettings) { s.MinFailedHosts = 0 })
	require.Equal(t, dynamicconfig.DefaultPeerHealthCheckSettings.MinFailedHosts, newPeerTestChecker(nil, nil, bad).minFailedHosts())

	good := testPeerHealthCheckConfig(0.8, 7, 1, 2*time.Second, 3*time.Second)
	c = newPeerTestChecker(nil, nil, good)
	require.Equal(t, 2*time.Second, c.probeTimeout())
	require.Equal(t, 3*time.Second, c.roundTimeout())
	require.InDelta(t, 0.8, c.samplePercentage(), 1e-9)
	require.InDelta(t, 1.0, c.failureThreshold(), 1e-9)
	require.Equal(t, 7, c.maxHostsPerRole())
	require.Equal(t, 2, c.minFailedHosts())
	good = withPeerSettings(good, func(s *dynamicconfig.PeerHealthCheckSettings) { s.MinFailedHosts = 3 })
	require.Equal(t, 3, newPeerTestChecker(nil, nil, good).minFailedHosts())

	// A probe timeout at or above the round timeout falls back below it, so stuck hosts still count as timed out.
	require.Equal(t, 1500*time.Millisecond, newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, 3*time.Second, 3*time.Second)).probeTimeout())
	require.Equal(t, 15*time.Second, newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, 15*time.Second, time.Minute)).probeTimeout())
	require.Equal(t, 5*time.Second, newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, 15*time.Second, 10*time.Second)).probeTimeout())
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
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
	require.Equal(t, int32(2), calls[primitives.HistoryService].Load())
	require.Equal(t, int32(2), calls[primitives.MatchingService].Load())
}

func TestRunRound_TimeoutsAtThresholdFailPerRole(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	// History times out 2/4 = 50%, at the threshold. Combined across roles it would be 2/8 and pass.
	probe := timingOutAddresses("history-0:7234", "history-1:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
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
	checker := newPeerTestChecker(monitor, timingOutAddresses(timingOut...), testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
}

func TestRunRound_MinFailedHostsPreventsFailure(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 3),
		primitives.MatchingService: testPeerHosts("matching", 3),
	})
	// One of two probes times out, reaching the 50% threshold but falling short of the two-timeout minimum.
	var historyCalls atomic.Int32
	probe := func(_ context.Context, role primitives.ServiceName, _ string) error {
		if role == primitives.HistoryService && historyCalls.Add(1) == 1 {
			return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
		}
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
	require.Equal(t, int32(2), historyCalls.Load())
}

func TestRunRound_ProbeDeadlineCountsAsTimeout(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	// history-0 and history-1 never answer. Only their own probe deadline can end them.
	probe := func(ctx context.Context, _ primitives.ServiceName, address string) error {
		if address == "history-0:7234" || address == "history-1:7234" {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	// The probe timeout is far below the round timeout, so the probe deadline fires first.
	checker := newPeerHealthChecker(monitor, probe, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 20, 0.5, 20*time.Millisecond, 10*time.Second), handler, log.NewNoopLogger())

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
	counts := probeMetricCounts(capture)
	require.Equal(t, int64(2), counts["history/timeout"])
	require.Equal(t, int64(2), counts["history/response"])
}

func TestRemoteDeadlineExceededCountsAsResponse(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(nil, func(context.Context, primitives.ServiceName, string) error {
		return serviceerror.NewDeadlineExceeded("remote deadline")
	}, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second), handler, log.NewNoopLogger())

	result := checker.runProbe(context.Background(), peerProbeTarget{role: primitives.HistoryService, address: "history:7234"})
	results := make(chan peerProbeResult, 1)
	results <- result
	counts, _ := checker.collectProbeResults(context.Background(), map[primitives.ServiceName]int{primitives.HistoryService: 1}, results)

	require.Equal(t, 1, counts[primitives.HistoryService][peerProbeResponse])
	require.Equal(t, map[string]int64{"history/response": 1}, probeMetricCounts(capture))
}

func TestProbeCutOffByEndedRoundIsUnknown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	checker := newPeerTestChecker(nil, func(context.Context, primitives.ServiceName, string) error { return nil }, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))
	result := checker.runProbe(ctx, peerProbeTarget{role: primitives.HistoryService, address: "history:7234"})
	require.Equal(t, peerProbeUnknown, result.outcome)
}

func TestRunRound_AllSampledTimeOutIsOutage(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	probe := func(context.Context, primitives.ServiceName, string) error {
		return status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.0.0.1:7234: i/o timeout"`)
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundPass, checker.runRound(context.Background()))
}

func TestRunRound_EmptyRoleIsIgnored(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: nil,
	})
	// History is still evaluated: 2/4 time out.
	probe := timingOutAddresses("history-0:7234", "history-1:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
}

func TestRunRound_MissingRoleWithHealthyOtherRoleIsUnknown(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  nil,
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	checker := newPeerTestChecker(monitor, timingOutAddresses(), testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

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
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundFail, checker.runRound(context.Background()))
}

func TestRunRound_NoRoleProbedIsUnknown(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  nil,
		primitives.MatchingService: nil,
	})
	checker := newPeerTestChecker(monitor, timingOutAddresses(), testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.Equal(t, peerRoundUnknown, checker.runRound(context.Background()))
}

func TestRunRound_DeadlineCountsUnfinishedAsUnknown(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 2),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	// The probe ignores its context, so only the round deadline can end the round.
	probe := func(_ context.Context, _ primitives.ServiceName, address string) error {
		if address == "history-0:7234" {
			<-block
		}
		return nil
	}
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(monitor, probe, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 20, 0.5, time.Minute, 50*time.Millisecond), handler, log.NewNoopLogger())

	// Responding probes may also miss the 50ms deadline on a busy machine, so assert only accounting and the blocked probe's unknown outcome.
	_ = checker.runRound(context.Background())
	counts := probeMetricCounts(capture)
	var total int64
	for _, n := range counts {
		total += n
	}
	require.Equal(t, int64(4), total, "every planned probe is recorded once")
	require.GreaterOrEqual(t, counts["history/unknown"], int64(1), "the blocked probe is unknown")
}

func TestEvaluateRound_UnknownOutcomesPreventDownstreamOutagePass(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, time.Second))
	planned := map[primitives.ServiceName]int{primitives.HistoryService: 2, primitives.MatchingService: 2}
	counts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeUnknown: 1, peerProbeTimeout: 1},
		primitives.MatchingService: {peerProbeUnresolved: 2},
	}

	require.Equal(t, peerRoundUnknown, checker.evaluateRound(planned, counts, nil))
	completedOutageCounts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeTimeout: 2},
		primitives.MatchingService: {peerProbeUnresolved: 2},
	}
	require.Equal(t, peerRoundPass, checker.evaluateRound(planned, completedOutageCounts, nil))
}

func TestEvaluateRoundUnknownProbesCanChangeVerdict(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, time.Second))
	planned := map[primitives.ServiceName]int{primitives.HistoryService: 4, primitives.MatchingService: 1}
	counts := map[primitives.ServiceName]map[peerProbeOutcome]int{
		primitives.HistoryService:  {peerProbeResponse: 1, peerProbeTimeout: 1, peerProbeUnknown: 2},
		primitives.MatchingService: {peerProbeResponse: 1},
	}
	require.Equal(t, peerRoundUnknown, checker.evaluateRound(planned, counts, nil))
	counts[primitives.HistoryService] = map[peerProbeOutcome]int{peerProbeResponse: 3, peerProbeUnknown: 1}
	require.Equal(t, peerRoundPass, checker.evaluateRound(planned, counts, nil))
}

func TestCollectProbeResults_ExpiredRoundKeepsArrivedResults(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(nil, nil, func(healthpb.HealthCheckResponse_ServingStatus) {},
		testPeerHealthCheckConfig(1, 20, 0.5, time.Second, time.Second), handler, log.NewNoopLogger())

	planned := map[primitives.ServiceName]int{primitives.HistoryService: 4, primitives.MatchingService: 3}
	results := make(chan peerProbeResult, 7)
	for _, outcome := range []peerProbeOutcome{peerProbeResponse, peerProbeResponse, peerProbeTimeout, peerProbeTimeout} {
		results <- peerProbeResult{role: primitives.HistoryService, outcome: outcome}
	}
	// Only two of three matching probes report.
	results <- peerProbeResult{role: primitives.MatchingService, outcome: peerProbeResponse}
	results <- peerProbeResult{role: primitives.MatchingService, outcome: peerProbeResponse}
	// The round deadline has already passed, so ctx.Done() and results are both ready.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	counts, representativeErrors := checker.collectProbeResults(ctx, planned, results)

	require.Equal(t, peerRoundFail, checker.evaluateRound(planned, counts, representativeErrors))
	require.Equal(t, map[string]int64{
		"history/response":  2,
		"history/timeout":   2,
		"matching/response": 2,
		"matching/unknown":  1,
	}, probeMetricCounts(capture))
}

func TestCollectProbeResultsKeepsRepresentativeTimeoutError(t *testing.T) {
	checker := newPeerTestChecker(nil, nil, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, time.Second))
	permissionErr := serviceerror.NewPermissionDenied("application response", "")
	timeoutErr := errors.New("transport: Error while dialing: operation timed out")
	results := make(chan peerProbeResult, 2)
	results <- peerProbeResult{role: primitives.HistoryService, outcome: peerProbeResponse, err: permissionErr}
	results <- peerProbeResult{role: primitives.HistoryService, outcome: peerProbeTimeout, err: timeoutErr}

	_, representativeErrors := checker.collectProbeResults(context.Background(), map[primitives.ServiceName]int{primitives.HistoryService: 2}, results)

	require.Same(t, timeoutErr, representativeErrors[primitives.HistoryService])
}

func TestRunRound_ConcurrencyCapped(t *testing.T) {
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)

	entered := make(chan struct{}, 200)
	var inFlight, maxInFlight atomic.Int32
	probe := func(context.Context, primitives.ServiceName, string) error {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		entered <- struct{}{}
		<-gate
		inFlight.Add(-1)
		return nil
	}
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 100),
		primitives.MatchingService: testPeerHosts("matching", 100),
	})
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 100, 0.5, 5*time.Second, 10*time.Second))

	done := make(chan string, 1)
	go func() { done <- checker.runRound(context.Background()) }()

	// Every probe blocks on gate, so only the concurrency cap limits how many are in flight.
	for range peerHealthCheckMaxConcurrentProbes {
		<-entered
	}
	// Yield so a probe admitted beyond the cap would have a chance to start.
	for range 1000 {
		runtime.Gosched()
	}
	require.Equal(t, int32(peerHealthCheckMaxConcurrentProbes), inFlight.Load())

	release()
	require.Equal(t, peerRoundPass, <-done)
	require.Equal(t, int32(peerHealthCheckMaxConcurrentProbes), maxInFlight.Load())
}

func TestRunRound_ConcurrencyCapIncludesProbesFromEarlierRound(t *testing.T) {
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	entered := make(chan struct{}, 100)
	var inFlight atomic.Int32
	probe := func(context.Context, primitives.ServiceName, string) error {
		inFlight.Add(1)
		entered <- struct{}{}
		<-gate
		inFlight.Add(-1)
		return nil
	}
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 100),
		primitives.MatchingService: testPeerHosts("matching", 100),
	})
	cfg := testPeerHealthCheckConfig(1, 100, 0.5, time.Minute, time.Second)
	checker := newPeerTestChecker(monitor, probe, cfg)

	firstRoundCtx, cancelFirstRound := context.WithCancel(context.Background())
	firstRoundDone := make(chan string, 1)
	go func() { firstRoundDone <- checker.runRound(firstRoundCtx) }()
	startDeadline := time.NewTimer(5 * time.Second)
	defer startDeadline.Stop()
	for range peerHealthCheckMaxConcurrentProbes {
		select {
		case <-entered:
		case <-startDeadline.C:
			t.Fatal("timed out waiting for first round to fill the shared probe cap")
		}
	}
	require.Equal(t, int32(peerHealthCheckMaxConcurrentProbes), inFlight.Load())
	cancelFirstRound()
	select {
	case verdict := <-firstRoundDone:
		require.Equal(t, peerRoundUnknown, verdict)
	case <-time.After(5 * time.Second):
		t.Fatal("first round did not exit after cancellation")
	}
	require.Equal(t, peerRoundUnknown, checker.runRound(context.Background()))
	require.Empty(t, entered, "the second round must not start probes while earlier probes still hold all slots")

	release()
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
	cfg := testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second)
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
		testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second), handler, log.NewNoopLogger())

	checker.tick(context.Background())

	require.Empty(t, rec.list())
	rounds := capture.SnapshotMetric(metrics.FrontendPeerHealthCheckRound.Name())
	require.Len(t, rounds, 1)
	require.Equal(t, peerRoundUnknown, rounds[0].Tags["outcome"])
}

func TestStartStop_LoopRunsAndStops(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	healthy.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, 10*time.Millisecond, rec)

	checker.Start()
	await.RequireTrue(t, func() bool {
		last, ok := rec.last()
		return ok && last == healthpb.HealthCheckResponse_SERVING && probeCalls.Load() >= 3
	}, 5*time.Second, 5*time.Millisecond)

	checker.Stop()
	checker.Stop() // second call must be a no-op

	stoppedAt := len(rec.list())
	require.Never(t, func() bool { return len(rec.list()) != stoppedAt }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestStart_FirstRoundRunsImmediately(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	healthy.Store(true)
	var probeCalls atomic.Int32
	rec := &statusRecorder{}
	checker := newPeerLoopChecker(t, &enabled, &healthy, &probeCalls, time.Hour, rec)

	checker.Start()
	t.Cleanup(checker.Stop)

	await.RequireTrue(t, func() bool {
		last, ok := rec.last()
		return ok && last == healthpb.HealthCheckResponse_SERVING
	}, time.Second, 5*time.Millisecond)
}

func TestStop_InterruptsInFlightRound(t *testing.T) {
	var enabled, healthy atomic.Bool
	enabled.Store(true)
	healthy.Store(true)
	rec := &statusRecorder{}
	started := make(chan struct{}, 1)
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 2),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	// The probe would succeed, but it blocks until the round is cancelled.
	probe := func(ctx context.Context, _ primitives.ServiceName, _ string) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil
	}
	cfg := testPeerHealthCheckConfig(1, 20, 0.5, time.Minute, time.Minute)
	cfg.Enabled = enabled.Load
	cfg = withPeerSettings(cfg, func(s *dynamicconfig.PeerHealthCheckSettings) { s.Interval = time.Hour })
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	checker := newPeerHealthChecker(monitor, probe, rec.set, cfg, handler, log.NewNoopLogger())

	checker.Start()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first round never started probing")
	}
	checker.Stop()

	require.Empty(t, rec.list(), "a round interrupted by Stop must not set status")
	require.Empty(t, capture.SnapshotMetric(metrics.FrontendPeerHealthCheckRound.Name()), "a round interrupted by Stop must not emit a round metric")
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
	cfg := withPeerSettings(testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second), func(s *dynamicconfig.PeerHealthCheckSettings) {
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
