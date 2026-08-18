package rpc

import (
	"cmp"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/temporal/environment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

var _ common.RPCFactory = (*RPCFactory)(nil)

// Minimum interval between (traffic-triggered) sweeps of shut-down connections.
const internodeConnCleanupInterval = 30 * time.Minute

// RPCFactory is an implementation of common.RPCFactory interface
type RPCFactory struct {
	config         *config.Config
	serviceName    primitives.ServiceName
	logger         log.Logger
	metricsHandler metrics.Handler

	frontendURL       string
	frontendHTTPURL   string
	frontendHTTPPort  int
	frontendTLSConfig *tls.Config

	grpcListener             func() net.Listener
	tlsFactory               encryption.TLSConfigProvider
	commonDialOptions        []grpc.DialOption
	perServiceDialOptions    map[primitives.ServiceName][]grpc.DialOption
	tokenProvider            auth.TokenProvider
	authHeaderName           string
	requireRemoteClusterAuth bool
	monitor                  membership.Monitor
	// A OnceValues wrapper for createLocalFrontendHTTPClient.
	localFrontendClient      func() (*common.FrontendHTTPClient, error)
	internodeGRPCConnections struct {
		sync.RWMutex
		conns map[string]*grpc.ClientConn
	}
	internodeConnCleanupTicker *time.Ticker
	// A OnceValue wrapper for createLocalFrontendGRPCConnection.
	localFrontendGRPCConn func() *grpc.ClientConn

	// Remote frontend connections, keyed by rpcAddress. Dialing one address
	// must not block lookups for another.
	remoteFrontendGRPCConns sync.Map // map[string]*grpc.ClientConn

	// TODO: Remove these flags once the keepalive settings are rolled out
	EnableInternodeServerKeepalive bool
	EnableInternodeClientKeepalive bool
}

// NewFactory builds a new RPCFactory
// conforming to the underlying configuration
func NewFactory(
	cfg *config.Config,
	sName primitives.ServiceName,
	logger log.Logger,
	metricsHandler metrics.Handler,
	tlsProvider encryption.TLSConfigProvider,
	frontendURL string,
	frontendHTTPURL string,
	frontendHTTPPort int,
	frontendTLSConfig *tls.Config,
	commonDialOptions []grpc.DialOption,
	perServiceDialOptions map[primitives.ServiceName][]grpc.DialOption,
	monitor membership.Monitor,
	tokenProvider auth.TokenProvider,
) *RPCFactory {
	authHeaderName := "authorization"
	requireRemoteClusterAuth := false
	if cfg != nil {
		authHeaderName = cmp.Or(cfg.Global.Authorization.AuthHeaderName, authHeaderName)
		requireRemoteClusterAuth = cfg.Global.Authorization.RemoteClusterAuth.Require
	}
	f := &RPCFactory{
		config:                   cfg,
		serviceName:              sName,
		logger:                   logger,
		metricsHandler:           metricsHandler,
		frontendURL:              frontendURL,
		frontendHTTPURL:          frontendHTTPURL,
		frontendHTTPPort:         frontendHTTPPort,
		frontendTLSConfig:        frontendTLSConfig,
		tlsFactory:               tlsProvider,
		commonDialOptions:        commonDialOptions,
		perServiceDialOptions:    perServiceDialOptions,
		tokenProvider:            tokenProvider,
		authHeaderName:           authHeaderName,
		requireRemoteClusterAuth: requireRemoteClusterAuth,
		monitor:                  monitor,
	}
	f.grpcListener = sync.OnceValue(f.createGRPCListener)
	f.localFrontendClient = sync.OnceValues(f.createLocalFrontendHTTPClient)
	f.internodeGRPCConnections.conns = make(map[string]*grpc.ClientConn)
	f.internodeConnCleanupTicker = time.NewTicker(internodeConnCleanupInterval)
	f.localFrontendGRPCConn = sync.OnceValue(f.createLocalFrontendGRPCConnection)
	return f
}

func (d *RPCFactory) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if d.tlsFactory != nil {
		serverConfig, err := d.tlsFactory.GetFrontendServerConfig()
		if err != nil {
			return nil, err
		}
		if serverConfig == nil {
			return opts, nil
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(serverConfig)))
	}

	return opts, nil
}

func (d *RPCFactory) GetFrontendClientTlsConfig() (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetFrontendClientConfig()
	}

	return nil, nil
}

func (d *RPCFactory) GetRemoteClusterClientConfig(hostname string) (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetRemoteClusterClientConfig(hostname)
	}

	return nil, nil
}

func (d *RPCFactory) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	var opts []grpc.ServerOption

	if d.EnableInternodeServerKeepalive {
		rpcConfig := d.config.Services[string(d.serviceName)].RPC
		kep := rpcConfig.KeepAliveServerConfig.GetKeepAliveEnforcementPolicy()
		kp := rpcConfig.KeepAliveServerConfig.GetKeepAliveServerParameters()
		opts = append(opts, grpc.KeepaliveEnforcementPolicy(kep), grpc.KeepaliveParams(kp))
	}
	if d.tlsFactory != nil {
		serverConfig, err := d.tlsFactory.GetInternodeServerConfig()
		if err != nil {
			return nil, err
		}
		if serverConfig == nil {
			return opts, nil
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(serverConfig)))
	}

	return opts, nil
}

func (d *RPCFactory) GetInternodeClientTlsConfig() (*tls.Config, error) {
	if d.tlsFactory != nil {
		return d.tlsFactory.GetInternodeClientConfig()
	}

	return nil, nil
}

// GetGRPCListener returns cached dispatcher for gRPC inbound or creates one
func (d *RPCFactory) GetGRPCListener() net.Listener {
	return d.grpcListener()
}

func (d *RPCFactory) createGRPCListener() net.Listener {
	rpcConfig := d.config.Services[string(d.serviceName)].RPC
	hostAddress := net.JoinHostPort(getListenIP(&rpcConfig, d.logger).String(), convert.IntToString(rpcConfig.GRPCPort))

	grpcListener, err := net.Listen("tcp", hostAddress)
	if err != nil || grpcListener == nil || grpcListener.Addr() == nil {
		d.logger.Fatal("Failed to start gRPC listener", tag.Error(err), tag.Service(d.serviceName), tag.Address(hostAddress))
	}

	d.logger.Info("Created gRPC listener", tag.Service(d.serviceName), tag.Address(hostAddress))
	return grpcListener
}

func getListenIP(cfg *config.RPC, logger log.Logger) net.IP {
	if cfg.BindOnLocalHost && len(cfg.BindOnIP) > 0 {
		logger.Fatal("ListenIP failed, bindOnLocalHost and bindOnIP are mutually exclusive")
		return nil
	}

	if cfg.BindOnLocalHost {
		return net.ParseIP(environment.GetLocalhostIP())
	}

	if len(cfg.BindOnIP) > 0 {
		ip := net.ParseIP(cfg.BindOnIP)
		if ip != nil {
			return ip
		}
		logger.Fatal("ListenIP failed, unable to parse bindOnIP value", tag.Address(cfg.BindOnIP))
		return nil
	}
	ip, err := config.ListenIP()
	if err != nil {
		logger.Fatal("ListenIP failed", tag.Error(err))
		return nil
	}
	return ip
}

// CreateRemoteFrontendGRPCConnection returns the shared connection for cross-cluster
// calls to rpcAddress, dialing it on first use. Callers must not close it.
func (d *RPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	if conn, ok := d.remoteFrontendGRPCConns.Load(rpcAddress); ok {
		return conn.(*grpc.ClientConn) //nolint:revive // unchecked-type-assertion
	}

	conn := d.dialRemoteFrontendGRPCConnection(rpcAddress)
	// Fatal may return under a non-fatal logger; caching nil would wedge this address.
	if conn == nil {
		return nil
	}

	// Dialed outside the lock, so concurrent callers can race; the loser closes its own.
	actual, loaded := d.remoteFrontendGRPCConns.LoadOrStore(rpcAddress, conn)
	if loaded {
		_ = conn.Close()
	}
	return actual.(*grpc.ClientConn) //nolint:revive // unchecked-type-assertion
}

func (d *RPCFactory) dialRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		hostname, _, err2 := net.SplitHostPort(rpcAddress)
		if err2 != nil {
			d.logger.Fatal("Invalid rpcAddress for remote cluster", tag.Error(err2))
			return nil
		}
		tlsClientConfig, err = d.tlsFactory.GetRemoteClusterClientConfig(hostname)
		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}

	keepAliveOption := d.getClientKeepAliveConfig(primitives.FrontendService)
	additionalDialOptions := append([]grpc.DialOption{}, d.perServiceDialOptions[primitives.FrontendService]...)

	// requireRemoteClusterAuth is defense-in-depth: temporal/fx.go boot validation
	// rejects (require=true, tokenProvider=nil), but RPCFactory is also constructed in
	// tests where the boot path doesn't run.
	if d.tokenProvider != nil || d.requireRemoteClusterAuth {
		fetch := func(ctx context.Context) (string, error) {
			var token string
			if d.tokenProvider != nil {
				t, err := d.tokenProvider.GetToken(ctx, rpcAddress)
				if err != nil {
					return "", err
				}
				token = t
			}
			if token == "" && d.requireRemoteClusterAuth {
				return "", status.Error(codes.Unauthenticated, "no auth token available for outbound remote-cluster RPC")
			}
			return token, nil
		}
		creds := auth.NewTokenCredentials(d.authHeaderName, fetch)
		additionalDialOptions = append(additionalDialOptions, grpc.WithPerRPCCredentials(creds))
	}

	return d.dial(rpcAddress, tlsClientConfig, append(additionalDialOptions, keepAliveOption)...)
}

// CreateLocalFrontendGRPCConnection returns the shared connection for internal frontend
// calls, dialing it on first use. Callers must not close it.
func (d *RPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	return d.localFrontendGRPCConn()
}

func (d *RPCFactory) createLocalFrontendGRPCConnection() *grpc.ClientConn {
	additionalDialOptions := append([]grpc.DialOption{}, d.perServiceDialOptions[primitives.InternalFrontendService]...)

	return d.dial(d.frontendURL, d.frontendTLSConfig, additionalDialOptions...)
}

// createInternodeGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) createInternodeGRPCConnection(hostName string, serviceName primitives.ServiceName) *grpc.ClientConn {
	d.maybeCleanupInternodeConns()

	// Reuse the cached connection unless it has been shut down, in which case re-dial.
	d.internodeGRPCConnections.RLock()
	conn, ok := d.internodeGRPCConnections.conns[hostName]
	d.internodeGRPCConnections.RUnlock()
	if ok && conn.GetState() != connectivity.Shutdown {
		return conn
	}

	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		tlsClientConfig, err = d.tlsFactory.GetInternodeClientConfig()
		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}
	additionalDialOptions := append([]grpc.DialOption{}, d.perServiceDialOptions[serviceName]...)
	newConn := d.dial(hostName, tlsClientConfig, append(additionalDialOptions, d.getClientKeepAliveConfig(serviceName))...)
	if newConn == nil {
		return nil
	}

	d.internodeGRPCConnections.Lock()
	defer d.internodeGRPCConnections.Unlock()
	if existing, ok := d.internodeGRPCConnections.conns[hostName]; ok && existing.GetState() != connectivity.Shutdown {
		_ = newConn.Close()
		return existing
	}
	d.internodeGRPCConnections.conns[hostName] = newConn
	return newConn
}

// Triggers a sweep at most once per interval, off the connection path.
func (d *RPCFactory) maybeCleanupInternodeConns() {
	select {
	case <-d.internodeConnCleanupTicker.C:
		go d.cleanupInternodeConns()
	default:
	}
}

// Removes shut-down cached connections; never closes live ones.
func (d *RPCFactory) cleanupInternodeConns() {
	d.internodeGRPCConnections.Lock()
	defer d.internodeGRPCConnections.Unlock()
	for hostName, conn := range d.internodeGRPCConnections.conns {
		if conn.GetState() == connectivity.Shutdown {
			delete(d.internodeGRPCConnections.conns, hostName)
		}
	}
}

func (d *RPCFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return d.createInternodeGRPCConnection(rpcAddress, primitives.HistoryService)
}

func (d *RPCFactory) CreateMatchingGRPCConnection(rpcAddress string) *grpc.ClientConn {
	return d.createInternodeGRPCConnection(rpcAddress, primitives.MatchingService)
}

func (d *RPCFactory) dial(hostName string, tlsClientConfig *tls.Config, dialOptions ...grpc.DialOption) *grpc.ClientConn {
	dialOptions = append(d.commonDialOptions, dialOptions...)
	connection, err := Dial(hostName, tlsClientConfig, d.logger, d.metricsHandler, dialOptions...)
	if err != nil {
		d.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
		return nil
	}

	return connection
}

func (d *RPCFactory) Close() {
	d.internodeConnCleanupTicker.Stop()

	d.internodeGRPCConnections.Lock()
	for _, conn := range d.internodeGRPCConnections.conns {
		_ = conn.Close()
	}
	clear(d.internodeGRPCConnections.conns)
	d.internodeGRPCConnections.Unlock()

	d.remoteFrontendGRPCConns.Range(func(_, v any) bool {
		if conn, ok := v.(*grpc.ClientConn); ok {
			_ = conn.Close()
		}
		return true
	})

	_ = d.localFrontendGRPCConn().Close()
}

func (d *RPCFactory) getClientKeepAliveConfig(serviceName primitives.ServiceName) grpc.DialOption {
	// default keepalive settings for clients
	params := keepalive.ClientParameters{
		Time:                time.Duration(math.MaxInt64),
		Timeout:             20 * time.Second,
		PermitWithoutStream: false,
	}
	if d.EnableInternodeClientKeepalive {
		serviceConfig := d.config.Services[string(serviceName)]
		params = serviceConfig.RPC.ClientConnectionConfig.GetKeepAliveClientParameters()
	}
	return grpc.WithKeepaliveParams(params)
}

func (d *RPCFactory) GetTLSConfigProvider() encryption.TLSConfigProvider {
	return d.tlsFactory
}

// CreateLocalFrontendHTTPClient gets or creates a cached frontend client.
func (d *RPCFactory) CreateLocalFrontendHTTPClient() (*common.FrontendHTTPClient, error) {
	return d.localFrontendClient()
}

// createLocalFrontendHTTPClient creates an HTTP client for communicating with the frontend.
// It uses either the provided frontendURL or membership to resolve the frontend address.
func (d *RPCFactory) createLocalFrontendHTTPClient() (*common.FrontendHTTPClient, error) {
	transport, err := common.NewHTTPTransport(d.frontendTLSConfig)
	if err != nil {
		return nil, err
	}
	client := http.Client{}

	// Default to http unless TLS is configured.
	scheme := "http"
	if d.frontendTLSConfig != nil {
		scheme = "https"
	}

	var address string
	if r := serviceResolverFromGRPCURL(d.frontendHTTPURL); r != nil {
		client.Transport = &roundTripper{
			resolver:   r,
			underlying: transport,
			httpPort:   d.frontendHTTPPort,
		}
		address = "internal" // This will be replaced by the roundTripper
	} else {
		// Use the URL as-is and leave the transport unmodified.
		client.Transport = transport
		address = d.frontendHTTPURL
	}

	return &common.FrontendHTTPClient{
		Client:  client,
		Address: address,
		Scheme:  scheme,
	}, nil
}

type roundTripper struct {
	resolver   membership.ServiceResolver
	underlying http.RoundTripper
	httpPort   int
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Pick a frontend host at random.
	members := rt.resolver.AvailableMembers()
	if len(members) == 0 {
		return nil, serviceerror.NewUnavailable("no frontend host to route request to")
	}
	idx := rand.Intn(len(members))
	member := members[idx]

	// Replace port with the HTTP port.
	host, _, err := net.SplitHostPort(member.Identity())
	if err != nil {
		return nil, fmt.Errorf("failed to extract port from frontend member: %w", err)
	}
	address := fmt.Sprintf("%s:%d", host, rt.httpPort)

	// Replace request's host.
	req.URL.Host = address
	req.Host = address
	return rt.underlying.RoundTrip(req)
}

// serviceResolverFromGRPCURL returns a ServiceResolver if ustr corresponds to a
// membership url, otherwise nil.
func serviceResolverFromGRPCURL(ustr string) membership.ServiceResolver {
	u, err := url.Parse(ustr)
	if err != nil {
		return nil
	}
	res, err := membership.GetServiceResolverFromURL(u)
	if err != nil {
		return nil
	}
	return res
}
