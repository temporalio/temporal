package testcore

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/client"
	matchingclient "go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

type clients struct {
	logger            log.Logger
	hostsByService    map[primitives.ServiceName]static.Hosts
	tlsConfigProvider *encryption.FixedTLSConfigProvider
	metricsHandler    metrics.Handler
	dcClient          *dynamicconfig.MemoryClient
	testHooks         testhooks.TestHooks
	numHistoryShards  int32
	metadataMgr       persistence.MetadataManager
	tokenProvider     auth.TokenProvider

	frontend frontendClients
	history  historyClients
	matching matchingClient
}

type frontendClients struct {
	once     sync.Once
	conn     *grpc.ClientConn
	admin    adminservice.AdminServiceClient
	frontend workflowservice.WorkflowServiceClient
	operator operatorservice.OperatorServiceClient
}

type historyClients struct {
	once      sync.Once
	conn      *grpc.ClientConn
	history   historyservice.HistoryServiceClient
	scheduler schedulerpb.SchedulerServiceClient
}

type matchingClient struct {
	once   sync.Once
	client matchingservice.MatchingServiceClient
}

func newClients(
	logger log.Logger,
	hostsByService map[primitives.ServiceName]static.Hosts,
	tlsConfigProvider *encryption.FixedTLSConfigProvider,
	metricsHandler metrics.Handler,
	dcClient *dynamicconfig.MemoryClient,
	testHooks testhooks.TestHooks,
	numHistoryShards int32,
	metadataMgr persistence.MetadataManager,
	tokenProvider auth.TokenProvider,
) clients {
	return clients{
		logger:            logger,
		hostsByService:    hostsByService,
		tlsConfigProvider: tlsConfigProvider,
		metricsHandler:    metricsHandler,
		dcClient:          dcClient,
		testHooks:         testHooks,
		numHistoryShards:  numHistoryShards,
		metadataMgr:       metadataMgr,
		tokenProvider:     tokenProvider,
	}
}

func (c *clients) AdminClient() adminservice.AdminServiceClient {
	c.ensureFrontend()
	return c.frontend.admin
}

func (c *clients) OperatorClient() operatorservice.OperatorServiceClient {
	c.ensureFrontend()
	return c.frontend.operator
}

func (c *clients) FrontendClient() workflowservice.WorkflowServiceClient {
	c.ensureFrontend()
	return c.frontend.frontend
}

func (c *clients) ensureFrontend() {
	c.frontend.once.Do(func() {
		conn, err := c.newConn(primitives.FrontendService)
		if err != nil {
			c.logger.Fatal("unable to create frontend test client", tag.Error(err))
		}
		c.frontend.conn = conn
		c.frontend.admin = adminservice.NewAdminServiceClient(conn)
		c.frontend.frontend = workflowservice.NewWorkflowServiceClient(conn)
		c.frontend.operator = operatorservice.NewOperatorServiceClient(conn)
	})
}

func (c *clients) HistoryClient() historyservice.HistoryServiceClient {
	c.ensureHistory()
	return c.history.history
}

func (c *clients) SchedulerClient() schedulerpb.SchedulerServiceClient {
	c.ensureHistory()
	return c.history.scheduler
}

func (c *clients) ensureHistory() {
	c.history.once.Do(func() {
		conn, err := c.newConn(primitives.HistoryService)
		if err != nil {
			c.logger.Fatal("unable to create history test client", tag.Error(err))
		}
		c.history.conn = conn
		c.history.history = historyservice.NewHistoryServiceClient(conn)
		c.history.scheduler = schedulerpb.NewSchedulerServiceClient(conn)
	})
}

func (c *clients) MatchingClient() matchingservice.MatchingServiceClient {
	c.ensureMatching()
	return c.matching.client
}

func (c *clients) ensureMatching() {
	c.matching.once.Do(func() {
		var tlsConfigProvider encryption.TLSConfigProvider
		var frontendTLSConfig *tls.Config
		if c.tlsConfigProvider != nil {
			var err error
			tlsConfigProvider = c.tlsConfigProvider
			frontendTLSConfig, err = c.tlsConfigProvider.GetFrontendClientConfig()
			if err != nil {
				c.logger.Fatal("failed getting client TLS config", tag.Error(err))
			}
		}

		monitor := static.NewMonitor(c.hostsByService)
		monitor.Start()
		frontendMembershipAddress := membership.GRPCResolverURLForTesting(monitor, primitives.FrontendService)
		rpcFactory := rpc.NewFactory(
			&config.Config{},
			primitives.FrontendService,
			c.logger,
			c.metricsHandler,
			tlsConfigProvider,
			frontendMembershipAddress,
			frontendMembershipAddress,
			0,
			frontendTLSConfig,
			nil,
			nil,
			monitor,
			c.tokenProvider,
		)
		clientFactory := client.NewFactoryProvider().NewFactory(
			rpcFactory,
			monitor,
			c.metricsHandler,
			dynamicconfig.NewCollection(c.dcClient, c.logger),
			c.testHooks,
			c.numHistoryShards,
			c.logger,
			c.logger,
		)
		namespaceIDToName := func(id namespace.ID) (namespace.Name, error) {
			resp, err := c.metadataMgr.GetNamespace(NewContext(), &persistence.GetNamespaceRequest{ID: id.String()})
			if err != nil {
				return "", err
			}
			return namespace.Name(resp.Namespace.Info.Name), nil
		}
		matchingClient, err := clientFactory.NewMatchingClientWithTimeout(
			namespaceIDToName,
			matchingclient.DefaultTimeout,
			matchingclient.DefaultLongPollTimeout,
		)
		if err != nil {
			c.logger.Fatal("unable to create matching test client", tag.Error(err))
		}
		c.matching.client = matchingClient
	})
}

func (c *clients) close() []error {
	var errs []error
	for _, conn := range []*grpc.ClientConn{
		c.frontend.conn,
		c.history.conn,
	} {
		if conn != nil {
			errs = append(errs, conn.Close())
		}
	}
	c.frontend.conn = nil
	c.history.conn = nil
	return errs
}

func (c *clients) newConn(serviceName primitives.ServiceName) (*grpc.ClientConn, error) {
	address, err := c.grpcAddress(serviceName)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := c.tlsConfig(serviceName)
	if err != nil {
		return nil, err
	}

	return rpc.Dial(address, tlsConfig, c.logger, metrics.NoopMetricsHandler)
}

func (c *clients) grpcAddress(serviceName primitives.ServiceName) (string, error) {
	hosts := c.hostsByService[serviceName].All
	if len(hosts) == 0 {
		return "", fmt.Errorf("no %s gRPC hosts configured", serviceName)
	}
	return hosts[0], nil
}

func (c *clients) tlsConfig(serviceName primitives.ServiceName) (*tls.Config, error) {
	if c.tlsConfigProvider == nil {
		return nil, nil
	}
	if serviceName == primitives.FrontendService {
		return c.tlsConfigProvider.GetFrontendClientConfig()
	}
	return c.tlsConfigProvider.GetInternodeClientConfig()
}

func newClientFactoryProvider(
	clusterConfig *cluster.Config,
	mockAdminClient map[string]adminservice.AdminServiceClient,
) client.FactoryProvider {
	return &clientFactoryProvider{
		config:          clusterConfig,
		mockAdminClient: mockAdminClient,
	}
}

type clientFactoryProvider struct {
	config          *cluster.Config
	mockAdminClient map[string]adminservice.AdminServiceClient
}

func (p *clientFactoryProvider) NewFactory(
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	testHooks testhooks.TestHooks,
	numberOfHistoryShards int32,
	logger log.Logger,
	throttledLogger log.Logger,
) client.Factory {
	f := client.NewFactoryProvider().NewFactory(
		rpcFactory,
		monitor,
		metricsHandler,
		dc,
		testHooks,
		numberOfHistoryShards,
		logger,
		throttledLogger,
	)
	return &clientFactory{
		Factory:         f,
		config:          p.config,
		mockAdminClient: p.mockAdminClient,
	}
}

type clientFactory struct {
	client.Factory
	config          *cluster.Config
	mockAdminClient map[string]adminservice.AdminServiceClient
}

// override just this one and look up connections in mock admin client map
func (f *clientFactory) NewRemoteAdminClientWithTimeout(rpcAddress string, timeout time.Duration, largeTimeout time.Duration) adminservice.AdminServiceClient {
	var clusterName string
	for name, info := range f.config.ClusterInformation {
		if rpcAddress == info.RPCAddress {
			clusterName = name
		}
	}
	if mock, ok := f.mockAdminClient[clusterName]; ok {
		return mock
	}
	return f.Factory.NewRemoteAdminClientWithTimeout(rpcAddress, timeout, largeTimeout)
}

func sdkClientFactoryProvider(
	grpcResolver *membership.GRPCResolver,
	metricsHandler metrics.Handler,
	logger log.Logger,
	dc *dynamicconfig.Collection,
	tlsConfigProvider encryption.TLSConfigProvider,
) sdk.ClientFactory {
	var tlsConfig *tls.Config
	if tlsConfigProvider != nil {
		var err error
		if tlsConfig, err = tlsConfigProvider.GetFrontendClientConfig(); err != nil {
			panic(err)
		}
	}
	return sdk.NewClientFactory(
		grpcResolver.MakeURL(primitives.FrontendService),
		tlsConfig,
		metricsHandler,
		logger,
		dynamicconfig.WorkerStickyCacheSize.Get(dc),
	)
}
