package client

import (
	"time"

	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/frontend"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

const (
	clientKeyConnection = "client-key-connection"
)

type (
	// Factory can be used to create RPC clients for temporal services
	Factory interface {
		NewHistoryClient() (history.Client, error)
		NewMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matching.Client, error)
		NewFrontendClient(rpcAddress string) (frontend.Client, error)

		NewHistoryClientWithTimeout(timeout time.Duration) (history.Client, error)
		NewMatchingClientWithTimeout(namespaceIDToName NamespaceIDToNameFunc, timeout time.Duration, longPollTimeout time.Duration) (matching.Client, error)
		NewFrontendClientWithTimeout(rpcAddress string, timeout time.Duration, longPollTimeout time.Duration) (frontend.Client, error)
		NewAdminClientWithTimeout(rpcAddress string, timeout time.Duration) (admin.Client, error)
	}

	// NamespaceIDToNameFunc maps a namespaceID to namespace name. Returns error when mapping is not possible.
	NamespaceIDToNameFunc func(string) (string, error)

	rpcClientFactory struct {
		rpcFactory            common.RPCFactory
		monitor               membership.Monitor
		metricsClient         metrics.Client
		dynConfig             *dynamicconfig.Collection
		numberOfHistoryShards int
		logger                log.Logger
	}
)

// NewRPCClientFactory creates an instance of client factory that knows how to dispatch RPC calls.
func NewRPCClientFactory(
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	metricsClient metrics.Client,
	dc *dynamicconfig.Collection,
	numberOfHistoryShards int,
	logger log.Logger,
) Factory {
	return &rpcClientFactory{
		rpcFactory:            rpcFactory,
		monitor:               monitor,
		metricsClient:         metricsClient,
		dynConfig:             dc,
		numberOfHistoryShards: numberOfHistoryShards,
		logger:                logger,
	}
}

func (cf *rpcClientFactory) NewHistoryClient() (history.Client, error) {
	return cf.NewHistoryClientWithTimeout(history.DefaultTimeout)
}

func (cf *rpcClientFactory) NewMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matching.Client, error) {
	return cf.NewMatchingClientWithTimeout(namespaceIDToName, matching.DefaultTimeout, matching.DefaultLongPollTimeout)
}

func (cf *rpcClientFactory) NewFrontendClient(rpcAddress string) (frontend.Client, error) {
	return cf.NewFrontendClientWithTimeout(rpcAddress, frontend.DefaultTimeout, frontend.DefaultLongPollTimeout)
}

func (cf *rpcClientFactory) NewHistoryClientWithTimeout(timeout time.Duration) (history.Client, error) {
	resolver, err := cf.monitor.GetResolver(common.HistoryServiceName)
	if err != nil {
		return nil, err
	}

	keyResolver := func(key string) (string, error) {
		host, err := resolver.Lookup(key)
		if err != nil {
			return "", err
		}
		return host.GetAddress(), nil
	}

	clientProvider := func(clientKey string) (interface{}, error) {
		connection := cf.rpcFactory.CreateGRPCConnection(clientKey)
		return historyservice.NewHistoryServiceClient(connection), nil
	}

	client := history.NewClient(cf.numberOfHistoryShards, timeout, common.NewClientCache(keyResolver, clientProvider), cf.logger)
	if cf.metricsClient != nil {
		client = history.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewMatchingClientWithTimeout(
	namespaceIDToName NamespaceIDToNameFunc,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (matching.Client, error) {
	resolver, err := cf.monitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	keyResolver := func(key string) (string, error) {
		host, err := resolver.Lookup(key)
		if err != nil {
			return "", err
		}
		return host.GetAddress(), nil
	}

	clientProvider := func(clientKey string) (interface{}, error) {
		connection := cf.rpcFactory.CreateGRPCConnection(clientKey)
		return matchingservice.NewMatchingServiceClient(connection), nil
	}

	client := matching.NewClient(
		timeout,
		longPollTimeout,
		common.NewClientCache(keyResolver, clientProvider),
		matching.NewLoadBalancer(namespaceIDToName, cf.dynConfig),
	)

	if cf.metricsClient != nil {
		client = matching.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil

}

func (cf *rpcClientFactory) NewFrontendClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (frontend.Client, error) {
	keyResolver := func(key string) (string, error) {
		return clientKeyConnection, nil
	}

	clientProvider := func(clientKey string) (interface{}, error) {
		connection := cf.rpcFactory.CreateGRPCConnection(rpcAddress)
		return workflowservice.NewWorkflowServiceClient(connection), nil
	}

	client := frontend.NewClient(timeout, longPollTimeout, common.NewClientCache(keyResolver, clientProvider))
	if cf.metricsClient != nil {
		client = frontend.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewAdminClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
) (admin.Client, error) {
	keyResolver := func(key string) (string, error) {
		return clientKeyConnection, nil
	}

	clientProvider := func(clientKey string) (interface{}, error) {
		connection := cf.rpcFactory.CreateGRPCConnection(rpcAddress)
		return adminservice.NewAdminServiceClient(connection), nil
	}

	client := admin.NewClient(timeout, common.NewClientCache(keyResolver, clientProvider))
	if cf.metricsClient != nil {
		client = admin.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}
