//go:build test_dep

// Package failpointserver serves the FailpointService on a server's gRPC server in test builds, so
// that tests and tools outside the process can configure its failpoints.
package failpointserver

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"sync"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/testx/failpoint/failpointgrpc"
	"go.temporal.io/testx/failpoint/failpointpb"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Module serves the FailpointService for the process's failpoints on its gRPC server.
//
// Services include it after their ServiceLifetimeHooks: fx runs stop hooks in reverse order, so
// the outstanding WaitForBlock calls are cancelled before the gRPC server drains its requests.
var Module = fx.Invoke(Register)

// FanOutModule serves a FailpointService on the gRPC server that applies each request to the
// process's failpoints and to those of every other host of the cluster; see RegisterFanOut. Like
// Module, it must be included after the service's ServiceLifetimeHooks.
var FanOutModule = fx.Invoke(RegisterFanOut)

// fanOutServices are the services whose hosts a fan-out forwards requests to.
var fanOutServices = []primitives.ServiceName{
	primitives.FrontendService,
	primitives.InternalFrontendService,
	primitives.HistoryService,
	primitives.MatchingService,
	primitives.WorkerService,
}

// Default names of the headers that carry the caller's credentials; see the authorization config.
const (
	defaultAuthHeaderName      = "authorization"
	defaultAuthExtraHeaderName = "authorization-extras"
)

// Register serves the FailpointService for the failpoints of testHooks on server. It serves
// nothing if testHooks has no failpoint registry, such as zero-value test hooks.
func Register(server *grpc.Server, testHooks testhooks.TestHooks, lc fx.Lifecycle) {
	if testHooks.Failpoints() == nil {
		return
	}
	service := cancelWaitsOnStop(lc, failpointgrpc.NewServer(testHooks.Failpoints()))
	failpointpb.RegisterFailpointServiceServer(server, service)
}

// FanOutParams are the dependencies of RegisterFanOut.
type FanOutParams struct {
	fx.In

	Server            *grpc.Server
	TestHooks         testhooks.TestHooks
	ServiceName       primitives.ServiceName
	Config            *config.Config
	HostInfoProvider  membership.HostInfoProvider
	MembershipMonitor membership.Monitor
	TLSConfigProvider encryption.TLSConfigProvider `optional:"true"`
	Logger            log.Logger
	Lifecycle         fx.Lifecycle
}

// RegisterFanOut serves a fan-out FailpointService on the server, so one call toggles a failpoint
// in every process of the cluster.
//
// It is meant for the frontends: they are the hosts that tests and tools can reach, and a gRPC
// server can only serve one implementation of the service, so a frontend serves the fan-out
// instead of the local-only service. The fan-out applies each request to the frontend's own
// failpoints and forwards it to every member of the frontend, internal frontend, history,
// matching and worker services. This frontend is left out of the peers, and the fan-out applies
// each request once per registry, so the services that share this process's registry don't
// apply it twice. Forwarded requests are marked, so a frontend that receives one from another
// frontend only applies it locally instead of forwarding it again.
//
// The frontend authorizes requests like those of the AdminService: only system admins may
// configure failpoints (see api.FailpointServicePrefix). Forwarded requests carry the caller's
// authorization headers to peer frontends, which authorize them too, but not to the other
// services, which rely on internode mTLS. The internal frontend's claim mapper grants system admin
// to every caller, so it admits failpoint requests from anyone that can reach it; that is the
// trust model of all admin APIs on the internal frontend, and the service is only served in test
// builds.
//
// Forwarding between frontends that use different TLS configs from the internode ones is not
// covered by tests: test clusters run a single frontend.
//
// It serves nothing if the test hooks have no failpoint registry, such as zero-value test hooks.
//
// Peers are looked up in the membership on every request, once the membership is initialized.
// Frontends are dialed with the frontend client TLS config; all other services with the
// internode client TLS config.
func RegisterFanOut(params FanOutParams) {
	if params.TestHooks.Failpoints() == nil {
		return
	}
	self := params.HostInfoProvider.HostInfo().GetAddress()
	peers := &peers{
		serviceName: params.ServiceName,
		self:        self,
		monitor:     params.MembershipMonitor,
		testHooks:   params.TestHooks,
		tlsProvider: params.TLSConfigProvider,
		logger:      params.Logger,
		conns:       make(map[peer]*grpc.ClientConn),
	}
	params.Lifecycle.Append(fx.StopHook(peers.close))

	local := failpointgrpc.Target{
		Host:    hostName(params.ServiceName, self),
		Service: failpointgrpc.NewServer(params.TestHooks.Failpoints()),
	}
	authConfig := params.Config.Global.Authorization
	fanOut := failpointgrpc.NewFanOut(local, peers.targets, failpointgrpc.WithForwardedHeaders(
		cmp.Or(authConfig.AuthHeaderName, defaultAuthHeaderName),
		cmp.Or(authConfig.AuthExtraHeaderName, defaultAuthExtraHeaderName),
	))
	failpointpb.RegisterFailpointServiceServer(params.Server, cancelWaitsOnStop(params.Lifecycle, fanOut))
}

// cancelWaitsOnStop cancels the outstanding WaitForBlock calls of service when the service
// stops, so a wait for a block that is never reached doesn't stall the gRPC server's drain.
func cancelWaitsOnStop(lc fx.Lifecycle, service failpointpb.FailpointServiceServer) failpointpb.FailpointServiceServer {
	stopCtx, stop := context.WithCancel(context.Background())
	lc.Append(fx.StopHook(stop))
	return &waitCanceler{FailpointServiceServer: service, stopCtx: stopCtx}
}

type waitCanceler struct {
	failpointpb.FailpointServiceServer
	stopCtx context.Context
}

func (w *waitCanceler) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopWaiting := context.AfterFunc(w.stopCtx, cancel)
	defer stopWaiting()
	return w.FailpointServiceServer.WaitForBlock(ctx, req)
}

// peer is a host that a fan-out forwards requests to.
type peer struct {
	service primitives.ServiceName
	address string
}

func hostName(service primitives.ServiceName, address string) string {
	return fmt.Sprintf("%s/%s", service, address)
}

// peerHosts returns the members of services, except the host at self of service.
func peerHosts(monitor membership.Monitor, services []primitives.ServiceName, service primitives.ServiceName, self string) []peer {
	var hosts []peer
	for _, peerService := range services {
		resolver, err := monitor.GetResolver(peerService)
		if err != nil {
			// The service isn't part of this cluster, such as an internal frontend.
			continue
		}
		for _, member := range resolver.Members() {
			address := member.GetAddress()
			if peerService == service && address == self {
				continue
			}
			hosts = append(hosts, peer{service: peerService, address: address})
		}
	}
	return hosts
}

type peers struct {
	serviceName primitives.ServiceName
	self        string
	monitor     membership.Monitor
	testHooks   testhooks.TestHooks
	tlsProvider encryption.TLSConfigProvider
	logger      log.Logger

	// mu serializes lookups, so each lookup reconciles the cache with a membership snapshot that
	// is at least as new as the one of the lookup before.
	mu sync.Mutex
	// conns caches a connection per peer. Connections of peers that left the membership are
	// closed on the next lookup, which fails the calls to them that are still in flight.
	conns map[peer]*grpc.ClientConn
	// closed is set when the service stops; lookups fail from then on.
	closed bool
}

// errClosed is returned by lookups after the service stopped.
var errClosed = errors.New("failpoint fan-out is stopped")

func (p *peers) targets(ctx context.Context) ([]failpointgrpc.Target, error) {
	// Before the membership is initialized, it lists no peers, and the request would only be
	// applied locally.
	if err := p.monitor.WaitUntilInitialized(ctx); err != nil {
		return nil, fmt.Errorf("waiting for membership: %w", err)
	}
	services := fanOutServices
	if enabled, ok := testhooks.Get(p.testHooks, testhooks.FailpointFanOutServices, testhooks.GlobalScope); ok {
		services = slices.DeleteFunc(slices.Clone(services), func(s primitives.ServiceName) bool {
			return !slices.Contains(enabled, s)
		})
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errClosed
	}
	// The snapshot is taken under the lock, so an older snapshot can't evict a connection that a
	// lookup with a newer one created.
	hosts := peerHosts(p.monitor, services, p.serviceName, p.self)
	p.closeDeparted(hosts)
	targets := make([]failpointgrpc.Target, 0, len(hosts))
	for _, host := range hosts {
		target := failpointgrpc.Target{
			Host: hostName(host.service, host.address),
			// Only frontends authorize the caller; the other services rely on internode mTLS.
			ForwardHeaders: host.service == primitives.FrontendService || host.service == primitives.InternalFrontendService,
		}
		if conn, err := p.connLocked(host); err != nil {
			target.Err = err
		} else {
			target.Service = failpointgrpc.NewRemote(conn)
		}
		targets = append(targets, target)
	}
	return targets, nil
}

// connLocked returns the cached connection to host, creating it on first use. The caller must
// hold p.mu.
func (p *peers) connLocked(host peer) (*grpc.ClientConn, error) {
	if conn, ok := p.conns[host]; ok {
		return conn, nil
	}
	tlsConfig, err := p.tlsConfig(host.service)
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(host.address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	p.conns[host] = conn
	return conn, nil
}

// closeDeparted closes the connections to peers that are not in hosts. The caller must hold p.mu.
func (p *peers) closeDeparted(hosts []peer) {
	for host, conn := range p.conns {
		if !slices.Contains(hosts, host) {
			p.closeConn(host, conn)
			delete(p.conns, host)
		}
	}
}

func (p *peers) closeConn(host peer, conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		p.logger.Warn("failed to close failpoint fan-out connection",
			tag.Address(hostName(host.service, host.address)), tag.Error(err))
	}
}

func (p *peers) tlsConfig(service primitives.ServiceName) (*tls.Config, error) {
	if p.tlsProvider == nil {
		return nil, nil
	}
	if service == primitives.FrontendService {
		return p.tlsProvider.GetFrontendClientConfig()
	}
	return p.tlsProvider.GetInternodeClientConfig()
}

func (p *peers) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.closeDeparted(nil)
}
