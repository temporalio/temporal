package testcore

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"github.com/dgryski/go-farm"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

type clients struct {
	logger            log.Logger
	hostsByService    map[primitives.ServiceName]static.Hosts
	tlsConfigProvider *encryption.FixedTLSConfigProvider
	dc                *dynamicconfig.Collection
	testHooks         testhooks.TestHooks
	metricsHandler    metrics.Handler

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

	clientConnsLock sync.Mutex
	clientConns     []*grpc.ClientConn
}

func newClients(
	logger log.Logger,
	hostsByService map[primitives.ServiceName]static.Hosts,
	tlsConfigProvider *encryption.FixedTLSConfigProvider,
) clients {
	return clients{
		logger:            logger,
		hostsByService:    hostsByService,
		tlsConfigProvider: tlsConfigProvider,
	}
}

func (c *clients) enableMatchingClientRouting(
	dc *dynamicconfig.Collection,
	testHooks testhooks.TestHooks,
	metricsHandler metrics.Handler,
) {
	c.dc = dc
	c.testHooks = testHooks
	c.metricsHandler = metricsHandler
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
	if c.matching.client != nil {
		return c.matching.client
	}
	c.matching.once.Do(func() {
		client, err := c.newMatchingClient()
		if err != nil {
			c.logger.Fatal("unable to create matching test client", tag.Error(err))
		}
		c.matching.client = client
	})
	return c.matching.client
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
	c.matching.clientConnsLock.Lock()
	for _, conn := range c.matching.clientConns {
		errs = append(errs, conn.Close())
	}
	c.matching.clientConns = nil
	c.matching.clientConnsLock.Unlock()
	c.frontend.conn = nil
	c.history.conn = nil
	return errs
}

func (c *clients) newConn(serviceName primitives.ServiceName) (*grpc.ClientConn, error) {
	address, err := c.grpcAddress(serviceName)
	if err != nil {
		return nil, err
	}
	return c.newConnToAddress(serviceName, address)
}

func (c *clients) newConnToAddress(serviceName primitives.ServiceName, address string) (*grpc.ClientConn, error) {
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

func (c *clients) newMatchingClient() (matchingservice.MatchingServiceClient, error) {
	if c.dc == nil {
		return nil, errors.New("matching client routing is not configured")
	}

	resolver := newTestMatchingServiceResolver(c.hostsByService[primitives.MatchingService].All)
	if resolver.MemberCount() == 0 {
		return nil, errors.New("no matching gRPC hosts configured")
	}

	clientProvider := func(clientKey string) (any, func() error, error) {
		conn, err := c.newConnToAddress(primitives.MatchingService, clientKey)
		if err != nil {
			return nil, nil, err
		}
		c.matching.clientConnsLock.Lock()
		c.matching.clientConns = append(c.matching.clientConns, conn)
		c.matching.clientConnsLock.Unlock()
		return matchingservice.NewMatchingServiceClient(conn), conn.Close, nil
	}
	client := matching.NewClient(
		matching.DefaultTimeout,
		matching.DefaultLongPollTimeout,
		common.NewClientCache(&testMatchingClientKeyResolver{resolver: resolver}, clientProvider, c.logger),
		c.metricsHandler,
		c.logger,
		matching.NewLoadBalancer(c.namespaceIDToName, c.dc, c.testHooks),
		dynamicconfig.MatchingSpreadRoutingBatchSize.Get(c.dc),
		resolver,
		dynamicconfig.MatchingConnectionCloseDelay.Get(c.dc),
	)
	if c.metricsHandler != nil {
		client = matching.NewMetricClient(client, c.metricsHandler, c.logger, c.logger)
	}
	return client, nil
}

func (c *clients) namespaceIDToName(id namespace.ID) (namespace.Name, error) {
	resp, err := c.FrontendClient().DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Id: id.String(),
	})
	if err != nil {
		return "", err
	}
	return namespace.Name(resp.GetNamespaceInfo().GetName()), nil
}

type testMatchingClientKeyResolver struct {
	resolver *testMatchingServiceResolver
}

func (r *testMatchingClientKeyResolver) Lookup(key string, index int) (string, error) {
	hosts := r.resolver.LookupN(key, index+1)
	if len(hosts) == 0 {
		return "", membership.ErrInsufficientHosts
	}
	if index >= len(hosts) {
		index %= len(hosts)
	}
	return hosts[index].GetAddress(), nil
}

func (r *testMatchingClientKeyResolver) GetAllAddresses() ([]string, error) {
	var addresses []string
	for _, host := range r.resolver.Members() {
		addresses = append(addresses, host.GetAddress())
	}
	return addresses, nil
}

type testMatchingServiceResolver struct {
	mu        sync.Mutex
	hostInfos []membership.HostInfo
	listeners map[string]chan<- *membership.ChangedEvent
}

func newTestMatchingServiceResolver(hosts []string) *testMatchingServiceResolver {
	hostInfos := make([]membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		hostInfos = append(hostInfos, membership.NewHostInfoFromAddress(host))
	}
	return &testMatchingServiceResolver{
		hostInfos: hostInfos,
		listeners: make(map[string]chan<- *membership.ChangedEvent),
	}
}

func (r *testMatchingServiceResolver) Lookup(key string) (membership.HostInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.hostInfos) == 0 {
		return nil, membership.ErrInsufficientHosts
	}
	hash := int(farm.Fingerprint32([]byte(key)))
	return r.hostInfos[hash%len(r.hostInfos)], nil
}

func (r *testMatchingServiceResolver) LookupN(key string, _ int) []membership.HostInfo {
	host, err := r.Lookup(key)
	if err != nil {
		return nil
	}
	return []membership.HostInfo{host}
}

func (r *testMatchingServiceResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.listeners[name]; ok {
		return membership.ErrListenerAlreadyExist
	}
	r.listeners[name] = notifyChannel
	return nil
}

func (r *testMatchingServiceResolver) RemoveListener(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.listeners, name)
	return nil
}

func (r *testMatchingServiceResolver) MemberCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.hostInfos)
}

func (r *testMatchingServiceResolver) AvailableMemberCount() int {
	return r.MemberCount()
}

func (r *testMatchingServiceResolver) Members() []membership.HostInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]membership.HostInfo(nil), r.hostInfos...)
}

func (r *testMatchingServiceResolver) AvailableMembers() []membership.HostInfo {
	return r.Members()
}

func (r *testMatchingServiceResolver) RequestRefresh() {
}
