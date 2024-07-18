package frontend

import (
	"context"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/matching"
)

type (
	HealthCheckClient struct {
		clients     []healthpb.HealthClient
		serviceName string
		failureRate dynamicconfig.FloatPropertyFn
	}

	HealthCheckClientFactory struct {
		membershipMonitor membership.Monitor
		rpcFactory        common.RPCFactory
		config            *Config
		connectionCache   cache.Cache
	}

	healthCheckResponse struct {
		*healthpb.HealthCheckResponse
		err error
	}
)

func (c *HealthCheckClient) Check(ctx context.Context) enumsspb.HealthState {
	respCh := make(chan healthCheckResponse, len(c.clients))
	for _, client := range c.clients {
		go func(hc healthpb.HealthClient) {
			resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: c.serviceName})
			respCh <- healthCheckResponse{
				HealthCheckResponse: resp,
				err:                 err,
			}
		}(client)
	}

	var failureCount float64
	for i := 0; i < len(c.clients); i++ {
		resp := <-respCh
		if resp.err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			failureCount++
		}
	}

	close(respCh)
	if (failureCount / float64(len(c.clients))) < c.failureRate() {
		return enumsspb.HEALTH_STATE_NOT_SERVING
	}
	return enumsspb.HEALTH_STATE_SERVING
}

func NewHealthCheckClientFactory(
	membershipMonitor membership.Monitor,
	rpcFactory common.RPCFactory,
	config *Config,
) HealthCheckClientFactory {
	factory := HealthCheckClientFactory{
		membershipMonitor: membershipMonitor,
		rpcFactory:        rpcFactory,
		config:            config,
		connectionCache:   cache.NewLRU(500, nil),
	}
	return factory
}

func (f *HealthCheckClientFactory) GetFrontendClients() (*HealthCheckClient, error) {
	clients, err := f.getGRPCHealthClients(primitives.FrontendService)
	if err != nil {
		return nil, err
	}
	return &HealthCheckClient{
		clients:     clients,
		serviceName: WorkflowServiceName,
		failureRate: f.config.FrontendHealthCheckFailureRate,
	}, nil
}

func (f *HealthCheckClientFactory) GetMatchingClients() (*HealthCheckClient, error) {
	clients, err := f.getGRPCHealthClients(primitives.MatchingService)
	if err != nil {
		return nil, err
	}
	return &HealthCheckClient{
		clients:     clients,
		serviceName: matching.ServiceName,
		failureRate: f.config.MatchingHealthCheckFailureRate,
	}, nil
}

func (f *HealthCheckClientFactory) getOrCreateClientConn(addr string) *grpc.ClientConn {
	item := f.connectionCache.Get(addr)
	if item != nil {
		return item.(*grpc.ClientConn)
	}
	grpcConn := f.rpcFactory.CreateInternodeGRPCConnection(addr)
	f.connectionCache.Put(addr, grpcConn)
	return grpcConn
}

func (f *HealthCheckClientFactory) getGRPCHealthClients(serviceName primitives.ServiceName) ([]healthpb.HealthClient, error) {
	resolver, err := f.membershipMonitor.GetResolver(serviceName)
	if err != nil {
		return nil, err
	}
	availableHosts := resolver.AvailableMembers()
	var clients []healthpb.HealthClient
	for _, host := range availableHosts {
		connection := f.getOrCreateClientConn(host.GetAddress())
		healthClient := healthpb.NewHealthClient(connection)
		clients = append(clients, healthClient)
	}
	return clients, nil
}
