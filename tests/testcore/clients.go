package testcore

import (
	"crypto/tls"
	"fmt"
	"sync"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"google.golang.org/grpc"
)

type clients struct {
	logger            log.Logger
	hostsByService    map[primitives.ServiceName]static.Hosts
	tlsConfigProvider *encryption.FixedTLSConfigProvider
	newMatchingClient func() (matchingservice.MatchingServiceClient, error)

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
	newMatchingClient func() (matchingservice.MatchingServiceClient, error),
) clients {
	return clients{
		logger:            logger,
		hostsByService:    hostsByService,
		tlsConfigProvider: tlsConfigProvider,
		newMatchingClient: newMatchingClient,
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
