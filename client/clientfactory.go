// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_factory_mock.go

package client

import (
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
)

type (
	// Factory can be used to create RPC clients for temporal services
	Factory interface {
		NewHistoryClientWithTimeout(timeout time.Duration) (historyservice.HistoryServiceClient, error)
		NewMatchingClientWithTimeout(namespaceIDToName NamespaceIDToNameFunc, timeout time.Duration, longPollTimeout time.Duration) (matchingservice.MatchingServiceClient, error)
		NewRemoteFrontendClientWithTimeout(rpcAddress string, timeout time.Duration, longPollTimeout time.Duration) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient)
		NewLocalFrontendClientWithTimeout(timeout time.Duration, longPollTimeout time.Duration) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error)
		NewRemoteAdminClientWithTimeout(rpcAddress string, timeout time.Duration, largeTimeout time.Duration) adminservice.AdminServiceClient
		NewLocalAdminClientWithTimeout(timeout time.Duration, largeTimeout time.Duration) (adminservice.AdminServiceClient, error)
	}

	// FactoryProvider can be used to provide a customized client Factory implementation.
	FactoryProvider interface {
		NewFactory(
			rpcFactory common.RPCFactory,
			monitor membership.Monitor,
			metricsHandler metrics.Handler,
			dc *dynamicconfig.Collection,
			numberOfHistoryShards int32,
			logger log.Logger,
			throttledLogger log.Logger,
		) Factory
	}

	// NamespaceIDToNameFunc maps a namespaceID to namespace name. Returns error when mapping is not possible.
	NamespaceIDToNameFunc func(id namespace.ID) (namespace.Name, error)

	rpcClientFactory struct {
		rpcFactory            common.RPCFactory
		monitor               membership.Monitor
		metricsHandler        metrics.Handler
		dynConfig             *dynamicconfig.Collection
		numberOfHistoryShards int32
		logger                log.Logger
		throttledLogger       log.Logger
	}

	factoryProviderImpl struct {
	}

	serviceKeyResolverImpl struct {
		resolver membership.ServiceResolver
	}
)

// NewFactoryProvider creates a default implementation of FactoryProvider.
func NewFactoryProvider() FactoryProvider {
	return &factoryProviderImpl{}
}

// NewFactory creates an instance of client factory that knows how to dispatch RPC calls.
func (p *factoryProviderImpl) NewFactory(
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	numberOfHistoryShards int32,
	logger log.Logger,
	throttledLogger log.Logger,
) Factory {
	return &rpcClientFactory{
		rpcFactory:            rpcFactory,
		monitor:               monitor,
		metricsHandler:        metricsHandler,
		dynConfig:             dc,
		numberOfHistoryShards: numberOfHistoryShards,
		logger:                logger,
		throttledLogger:       throttledLogger,
	}
}

func (cf *rpcClientFactory) NewHistoryClientWithTimeout(timeout time.Duration) (historyservice.HistoryServiceClient, error) {
	resolver, err := cf.monitor.GetResolver(primitives.HistoryService)
	if err != nil {
		return nil, err
	}

	client := history.NewClient(
		cf.dynConfig,
		resolver,
		cf.logger,
		cf.numberOfHistoryShards,
		cf.rpcFactory,
		timeout,
	)
	if cf.metricsHandler != nil {
		client = history.NewMetricClient(client, cf.metricsHandler, cf.logger, cf.throttledLogger)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewMatchingClientWithTimeout(
	namespaceIDToName NamespaceIDToNameFunc,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (matchingservice.MatchingServiceClient, error) {
	resolver, err := cf.monitor.GetResolver(primitives.MatchingService)
	if err != nil {
		return nil, err
	}

	keyResolver := newServiceKeyResolver(resolver)
	clientProvider := func(clientKey string) (interface{}, error) {
		connection := cf.rpcFactory.CreateInternodeGRPCConnection(clientKey)
		return matchingservice.NewMatchingServiceClient(connection), nil
	}
	client := matching.NewClient(
		timeout,
		longPollTimeout,
		common.NewClientCache(keyResolver, clientProvider),
		cf.metricsHandler,
		cf.logger,
		matching.NewLoadBalancer(namespaceIDToName, cf.dynConfig),
	)

	if cf.metricsHandler != nil {
		client = matching.NewMetricClient(client, cf.metricsHandler, cf.logger, cf.throttledLogger)
	}
	return client, nil

}

func (cf *rpcClientFactory) NewRemoteFrontendClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient) {
	connection := cf.rpcFactory.CreateRemoteFrontendGRPCConnection(rpcAddress)
	client := workflowservice.NewWorkflowServiceClient(connection)
	return connection, cf.newFrontendClient(client, timeout, longPollTimeout)
}

func (cf *rpcClientFactory) NewLocalFrontendClientWithTimeout(
	timeout time.Duration,
	longPollTimeout time.Duration,
) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error) {
	connection := cf.rpcFactory.CreateLocalFrontendGRPCConnection()
	client := workflowservice.NewWorkflowServiceClient(connection)
	return connection, cf.newFrontendClient(client, timeout, longPollTimeout), nil
}

func (cf *rpcClientFactory) NewRemoteAdminClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
	largeTimeout time.Duration,
) adminservice.AdminServiceClient {
	connection := cf.rpcFactory.CreateRemoteFrontendGRPCConnection(rpcAddress)
	client := adminservice.NewAdminServiceClient(connection)
	return cf.newAdminClient(client, timeout, largeTimeout)
}

func (cf *rpcClientFactory) NewLocalAdminClientWithTimeout(
	timeout time.Duration,
	longPollTimeout time.Duration,
) (adminservice.AdminServiceClient, error) {
	connection := cf.rpcFactory.CreateLocalFrontendGRPCConnection()
	client := adminservice.NewAdminServiceClient(connection)
	return cf.newAdminClient(client, timeout, longPollTimeout), nil
}

func (cf *rpcClientFactory) newAdminClient(
	client adminservice.AdminServiceClient,
	timeout time.Duration,
	longPollTimeout time.Duration,
) adminservice.AdminServiceClient {
	client = admin.NewClient(timeout, longPollTimeout, client)
	if cf.metricsHandler != nil {
		client = admin.NewMetricClient(client, cf.metricsHandler, cf.throttledLogger)
	}
	return client
}

func (cf *rpcClientFactory) newFrontendClient(
	client workflowservice.WorkflowServiceClient,
	timeout time.Duration,
	longPollTimeout time.Duration,
) workflowservice.WorkflowServiceClient {
	client = frontend.NewClient(timeout, longPollTimeout, client)
	if cf.metricsHandler != nil {
		client = frontend.NewMetricClient(client, cf.metricsHandler, cf.throttledLogger)
	}
	return client
}

func newServiceKeyResolver(resolver membership.ServiceResolver) *serviceKeyResolverImpl {
	return &serviceKeyResolverImpl{
		resolver: resolver,
	}
}

func (r *serviceKeyResolverImpl) Lookup(key string) (string, error) {
	host, err := r.resolver.Lookup(key)
	if err != nil {
		return "", err
	}
	return host.GetAddress(), nil
}

func (r *serviceKeyResolverImpl) GetAllAddresses() ([]string, error) {
	var all []string

	for _, host := range r.resolver.Members() {
		all = append(all, host.GetAddress())
	}

	return all, nil
}
