package rpc

import (
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
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/temporal/environment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var _ common.RPCFactory = (*RPCFactory)(nil)

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

	grpcListener          func() net.Listener
	tlsFactory            encryption.TLSConfigProvider
	commonDialOptions     []grpc.DialOption
	perServiceDialOptions map[primitives.ServiceName][]grpc.DialOption
	monitor               membership.Monitor
	// A OnceValues wrapper for createLocalFrontendHTTPClient.
	localFrontendClient      func() (*common.FrontendHTTPClient, error)
	interNodeGrpcConnections cache.Cache

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
) *RPCFactory {
	f := &RPCFactory{
		config:                cfg,
		serviceName:           sName,
		logger:                logger,
		metricsHandler:        metricsHandler,
		frontendURL:           frontendURL,
		frontendHTTPURL:       frontendHTTPURL,
		frontendHTTPPort:      frontendHTTPPort,
		frontendTLSConfig:     frontendTLSConfig,
		tlsFactory:            tlsProvider,
		commonDialOptions:     commonDialOptions,
		perServiceDialOptions: perServiceDialOptions,
		monitor:               monitor,
	}
	f.grpcListener = sync.OnceValue(f.createGRPCListener)
	f.localFrontendClient = sync.OnceValues(f.createLocalFrontendHTTPClient)
	f.interNodeGrpcConnections = cache.NewSimple(nil)
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

// CreateRemoteFrontendGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) CreateRemoteFrontendGRPCConnection(rpcAddress string) *grpc.ClientConn {
	var tlsClientConfig *tls.Config
	var err error
	if d.tlsFactory != nil {
		hostname, _, err2 := net.SplitHostPort(rpcAddress)
		if err2 != nil {
			d.logger.Fatal("Invalid rpcAddress for remote cluster", tag.Error(err2))
		}
		tlsClientConfig, err = d.tlsFactory.GetRemoteClusterClientConfig(hostname)

		if err != nil {
			d.logger.Fatal("Failed to create tls config for gRPC connection", tag.Error(err))
			return nil
		}
	}
	keepAliveOption := d.getClientKeepAliveConfig(primitives.FrontendService)
	additionalDialOptions := append([]grpc.DialOption{}, d.perServiceDialOptions[primitives.FrontendService]...)

	return d.dial(rpcAddress, tlsClientConfig, append(additionalDialOptions, keepAliveOption)...)
}

// CreateLocalFrontendGRPCConnection creates connection for internal frontend calls
func (d *RPCFactory) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	additionalDialOptions := append([]grpc.DialOption{}, d.perServiceDialOptions[primitives.InternalFrontendService]...)

	return d.dial(d.frontendURL, d.frontendTLSConfig, additionalDialOptions...)
}

// createInternodeGRPCConnection creates connection for gRPC calls
func (d *RPCFactory) createInternodeGRPCConnection(hostName string, serviceName primitives.ServiceName) *grpc.ClientConn {
	if c, ok := d.interNodeGrpcConnections.Get(hostName).(*grpc.ClientConn); ok {
		return c
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
	c := d.dial(hostName, tlsClientConfig, append(additionalDialOptions, d.getClientKeepAliveConfig(serviceName))...)
	d.interNodeGrpcConnections.Put(hostName, c)
	return c
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
	// dialer and transport field values copied from http.DefaultTransport.
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := http.Client{}

	// Default to http unless TLS is configured.
	scheme := "http"
	if d.frontendTLSConfig != nil {
		transport.TLSClientConfig = d.frontendTLSConfig
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
