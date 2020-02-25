// Copyright (c) 2017 Uber Technologies, Inc.
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
	// Factory can be used to create RPC clients for cadence services
	Factory interface {
		NewHistoryClient() (history.Client, error)
		NewMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error)
		NewFrontendClient(rpcAddress string) (frontend.Client, error)

		NewHistoryClientWithTimeout(timeout time.Duration) (history.Client, error)
		NewMatchingClientWithTimeout(domainIDToName DomainIDToNameFunc, timeout time.Duration, longPollTimeout time.Duration) (matching.Client, error)
		NewFrontendClientWithTimeout(rpcAddress string, timeout time.Duration, longPollTimeout time.Duration) (frontend.Client, error)
		NewAdminClientWithTimeout(rpcAddress string, timeout time.Duration) (admin.Client, error)
	}

	// DomainIDToNameFunc maps a domainID to domain name. Returns error when mapping is not possible.
	DomainIDToNameFunc func(string) (string, error)

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

func (cf *rpcClientFactory) NewMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error) {
	return cf.NewMatchingClientWithTimeout(domainIDToName, matching.DefaultTimeout, matching.DefaultLongPollTimeout)
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
	domainIDToName DomainIDToNameFunc,
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
		matching.NewLoadBalancer(domainIDToName, cf.dynConfig),
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
