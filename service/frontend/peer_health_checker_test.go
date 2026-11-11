package frontend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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
		Enabled:          dynamicconfig.GetBoolPropertyFn(true),
		Interval:         dynamicconfig.GetDurationPropertyFn(time.Second),
		ProbeTimeout:     dynamicconfig.GetDurationPropertyFn(probeTimeout),
		RoundTimeout:     dynamicconfig.GetDurationPropertyFn(roundTimeout),
		SamplePercentage: dynamicconfig.GetFloatPropertyFn(pct),
		FailureThreshold: dynamicconfig.GetFloatPropertyFn(threshold),
		MaxHostsPerRole:  dynamicconfig.GetIntPropertyFn(maxHosts),
	}
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

// failingAddresses returns a probe that fails for the given addresses and succeeds otherwise.
func failingAddresses(failing ...string) peerProbeFn {
	set := make(map[string]bool, len(failing))
	for _, addr := range failing {
		set[addr] = true
	}
	return func(_ context.Context, _ primitives.ServiceName, address string) error {
		if set[address] {
			return errors.New("unreachable")
		}
		return nil
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

func TestRunRound_AllHealthy(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	var calls atomic.Int32
	probe := func(context.Context, primitives.ServiceName, string) error {
		calls.Add(1)
		return nil
	}
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.True(t, checker.runRound(context.Background()))
	require.Equal(t, int32(4), calls.Load())
}

func TestRunRound_RoleAtThresholdFails(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 2),
		primitives.MatchingService: testPeerHosts("matching", 2),
	})
	probe := failingAddresses("history-0:7234")
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.False(t, checker.runRound(context.Background()))
}

func TestRunRound_PerRoleNotCombined(t *testing.T) {
	matching := testPeerHosts("matching", 10)
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 100),
		primitives.MatchingService: matching,
	})
	// Every matching host fails, so any sample of matching hosts fails its role. History is all healthy.
	failing := make([]string, 0, len(matching))
	for _, h := range matching {
		failing = append(failing, h.GetAddress())
	}
	checker := newPeerTestChecker(monitor, failingAddresses(failing...), testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.False(t, checker.runRound(context.Background()))
}

func TestRunRound_BelowThresholdPasses(t *testing.T) {
	history := testPeerHosts("history", 20)
	failing := make([]string, 0, 9)
	for _, h := range history[:9] {
		failing = append(failing, h.GetAddress())
	}
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  history,
		primitives.MatchingService: testPeerHosts("matching", 4),
	})
	checker := newPeerTestChecker(monitor, failingAddresses(failing...), testPeerHealthCheckConfig(1, 20, 0.5, time.Second, 5*time.Second))

	require.True(t, checker.runRound(context.Background()))
}

func TestRunRound_EmptyRoleFails(t *testing.T) {
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 4),
		primitives.MatchingService: nil,
	})
	checker := newPeerTestChecker(monitor, failingAddresses(), testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.False(t, checker.runRound(context.Background()))
}

func TestRunRound_ResolverErrorFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	monitor := membership.NewMockMonitor(ctrl)
	historyResolver := membership.NewMockServiceResolver(ctrl)
	historyResolver.EXPECT().AvailableMembers().Return(testPeerHosts("history", 4)).AnyTimes()
	monitor.EXPECT().GetResolver(primitives.HistoryService).Return(historyResolver, nil).AnyTimes()
	monitor.EXPECT().GetResolver(primitives.MatchingService).Return(nil, errors.New("resolver unavailable")).AnyTimes()
	checker := newPeerTestChecker(monitor, failingAddresses(), testPeerHealthCheckConfig(0.5, 20, 0.5, time.Second, 5*time.Second))

	require.False(t, checker.runRound(context.Background()))
}

func TestRunRound_DeadlineExceededFails(t *testing.T) {
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
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 20, 0.5, time.Minute, 50*time.Millisecond))

	start := time.Now()
	require.False(t, checker.runRound(context.Background()))
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestRunRound_ConcurrencyCapped(t *testing.T) {
	var inFlight, maxInFlight atomic.Int32
	var mu sync.Mutex
	probe := func(context.Context, primitives.ServiceName, string) error {
		cur := inFlight.Add(1)
		mu.Lock()
		if cur > maxInFlight.Load() {
			maxInFlight.Store(cur)
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}
	monitor := newPeerTestMonitor(t, map[primitives.ServiceName][]membership.HostInfo{
		primitives.HistoryService:  testPeerHosts("history", 100),
		primitives.MatchingService: testPeerHosts("matching", 100),
	})
	checker := newPeerTestChecker(monitor, probe, testPeerHealthCheckConfig(1, 100, 0.5, 5*time.Second, 10*time.Second))

	require.True(t, checker.runRound(context.Background()))
	require.LessOrEqual(t, maxInFlight.Load(), int32(peerHealthCheckMaxConcurrentProbes))
}
