//go:build test_dep

package failpointserver

import (
	"context"
	"crypto/tls"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func newTestMonitor() membership.Monitor {
	monitor := static.NewMonitor(map[primitives.ServiceName]static.Hosts{
		primitives.FrontendService: {All: []string{"fe-1:7233", "fe-2:7233"}},
		primitives.HistoryService:  {All: []string{"history-1:7234"}},
		primitives.MatchingService: {All: []string{"matching-1:7235"}},
		primitives.WorkerService:   {All: []string{"worker-1:7239"}},
		// Not a fan-out service.
		primitives.ServiceName("other"): {All: []string{"other-1:1234"}},
	})
	monitor.Start()
	return monitor
}

func TestPeerHosts(t *testing.T) {
	t.Parallel()
	monitor := newTestMonitor()

	tests := []struct {
		name    string
		service primitives.ServiceName
		self    string
		want    []peer
	}{
		{
			name:    "frontend leaves itself out",
			service: primitives.FrontendService,
			self:    "fe-1:7233",
			want: []peer{
				{service: primitives.FrontendService, address: "fe-2:7233"},
				{service: primitives.HistoryService, address: "history-1:7234"},
				{service: primitives.MatchingService, address: "matching-1:7235"},
				{service: primitives.WorkerService, address: "worker-1:7239"},
			},
		},
		{
			name:    "same address of another service is a peer",
			service: primitives.InternalFrontendService,
			self:    "fe-1:7233",
			want: []peer{
				{service: primitives.FrontendService, address: "fe-1:7233"},
				{service: primitives.FrontendService, address: "fe-2:7233"},
				{service: primitives.HistoryService, address: "history-1:7234"},
				{service: primitives.MatchingService, address: "matching-1:7235"},
				{service: primitives.WorkerService, address: "worker-1:7239"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ElementsMatch(t, tc.want, peerHosts(monitor, fanOutServices, tc.service, tc.self))
		})
	}
}

// failingTLSProvider fails to provide the internode client TLS config.
type failingTLSProvider struct {
	*encryption.FixedTLSConfigProvider
}

func (failingTLSProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return nil, errors.New("no internode cert")
}

func newTestPeers(monitor membership.Monitor, testHooks testhooks.TestHooks, tlsProvider encryption.TLSConfigProvider) *peers {
	return &peers{
		serviceName: primitives.FrontendService,
		self:        "fe-1:7233",
		monitor:     monitor,
		testHooks:   testHooks,
		tlsProvider: tlsProvider,
		logger:      log.NewNoopLogger(),
		conns:       make(map[peer]*grpc.ClientConn),
	}
}

func targetHosts(targets []failpointgrpc.Target) []string {
	hosts := make([]string, 0, len(targets))
	for _, target := range targets {
		hosts = append(hosts, target.Host)
	}
	return hosts
}

func TestPeers_Targets(t *testing.T) {
	t.Parallel()

	t.Run("limits peers to the enabled services", func(t *testing.T) {
		t.Parallel()
		testHooks := testhooks.NewTestHooks()
		testhooks.Set(testHooks, testhooks.FailpointFanOutServices,
			[]primitives.ServiceName{primitives.FrontendService, primitives.HistoryService, primitives.MatchingService},
			testhooks.GlobalScope)
		p := newTestPeers(newTestMonitor(), testHooks, nil)
		t.Cleanup(p.close)

		targets, err := p.targets(t.Context())
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"frontend/fe-2:7233", "history/history-1:7234", "matching/matching-1:7235"}, targetHosts(targets))
	})

	t.Run("forwards credentials only to frontends", func(t *testing.T) {
		t.Parallel()
		p := newTestPeers(newTestMonitor(), testhooks.NewTestHooks(), nil)
		t.Cleanup(p.close)

		targets, err := p.targets(t.Context())
		require.NoError(t, err)
		forwardHeaders := map[string]bool{}
		for _, target := range targets {
			forwardHeaders[target.Host] = target.ForwardHeaders
		}
		require.Equal(t, map[string]bool{
			"frontend/fe-2:7233":       true,
			"history/history-1:7234":   false,
			"matching/matching-1:7235": false,
			"worker/worker-1:7239":     false,
		}, forwardHeaders)
	})

	t.Run("fails after the service stopped", func(t *testing.T) {
		t.Parallel()
		p := newTestPeers(newTestMonitor(), testhooks.NewTestHooks(), nil)
		_, err := p.targets(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, p.conns)

		p.close()
		_, err = p.targets(t.Context())
		require.ErrorIs(t, err, errClosed)
		require.Empty(t, p.conns)
	})

	t.Run("reports a peer that cannot be dialed as its failure", func(t *testing.T) {
		t.Parallel()
		p := newTestPeers(newTestMonitor(), testhooks.NewTestHooks(), failingTLSProvider{&encryption.FixedTLSConfigProvider{}})
		t.Cleanup(p.close)

		targets, err := p.targets(t.Context())
		require.NoError(t, err)
		require.Len(t, targets, 4)
		for _, target := range targets {
			if target.Host == "frontend/fe-2:7233" {
				require.NoError(t, target.Err)
				require.NotNil(t, target.Service)
				continue
			}
			require.ErrorContains(t, target.Err, "no internode cert", target.Host)
		}
	})

	t.Run("waits for the membership", func(t *testing.T) {
		t.Parallel()
		monitor := membership.NewMockMonitor(gomock.NewController(t))
		monitor.EXPECT().WaitUntilInitialized(gomock.Any()).DoAndReturn(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		p := newTestPeers(monitor, testhooks.NewTestHooks(), nil)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := p.targets(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("closes connections of departed peers", func(t *testing.T) {
		t.Parallel()
		p := newTestPeers(newTestMonitor(), testhooks.NewTestHooks(), nil)
		t.Cleanup(p.close)
		departed := peer{service: primitives.HistoryService, address: "history-2:7234"}
		conn, err := p.connLocked(departed)
		require.NoError(t, err)

		_, err = p.targets(t.Context())
		require.NoError(t, err)
		require.NotContains(t, p.conns, departed)
		require.Equal(t, connectivity.Shutdown, conn.GetState())
		require.Len(t, p.conns, 4)
	})
}

func TestRegister_WithoutRegistry(t *testing.T) {
	t.Parallel()
	const serviceName = "temporal.testx.failpoint.v1.FailpointService"
	tests := []struct {
		name      string
		testHooks testhooks.TestHooks
		want      bool
	}{
		{name: "test hooks", testHooks: testhooks.NewTestHooks(), want: true},
		{name: "zero-value test hooks", testHooks: testhooks.TestHooks{}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := grpc.NewServer()
			Register(server, tc.testHooks, fxtest.NewLifecycle(t))
			require.Equal(t, tc.want, slices.Contains(slices.Collect(maps.Keys(server.GetServiceInfo())), serviceName))

			fanOutServer := grpc.NewServer()
			RegisterFanOut(FanOutParams{
				Server:            fanOutServer,
				TestHooks:         tc.testHooks,
				ServiceName:       primitives.FrontendService,
				Config:            &config.Config{},
				HostInfoProvider:  membership.NewHostInfoProvider(membership.NewHostInfoFromAddress("fe-1:7233")),
				MembershipMonitor: newTestMonitor(),
				Logger:            log.NewNoopLogger(),
				Lifecycle:         fxtest.NewLifecycle(t),
			})
			require.Equal(t, tc.want, slices.Contains(slices.Collect(maps.Keys(fanOutServer.GetServiceInfo())), serviceName))
		})
	}
}
