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
	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"time"
)

// Factory can be used to create RPC clients for cadence services
type Factory interface {
	NewHistoryClient() (history.Client, error)
	NewMatchingClient() (matching.Client, error)
	NewFrontendClient() (frontend.Client, error)

	NewHistoryClientWithTimeout(timeout time.Duration) (history.Client, error)
	NewMatchingClientWithTimeout(timeout time.Duration, longPollTimeout time.Duration) (matching.Client, error)
	NewFrontendClientWithTimeout(timeout time.Duration) (frontend.Client, error)
}

type rpcClientFactory struct {
	df                    common.RPCFactory
	monitor               membership.Monitor
	metricsClient         metrics.Client
	numberOfHistoryShards int
}

// NewRPCClientFactory creates an instance of client factory that knows how to dispatch RPC calls.
func NewRPCClientFactory(df common.RPCFactory,
	monitor membership.Monitor, metricsClient metrics.Client, numberOfHistoryShards int) Factory {
	return &rpcClientFactory{
		df:                    df,
		monitor:               monitor,
		metricsClient:         metricsClient,
		numberOfHistoryShards: numberOfHistoryShards,
	}
}

func (cf *rpcClientFactory) NewHistoryClient() (history.Client, error) {
	return cf.NewHistoryClientWithTimeout(history.DefaultTimeout)
}

func (cf *rpcClientFactory) NewMatchingClient() (matching.Client, error) {
	return cf.NewMatchingClientWithTimeout(matching.DefaultTimeout, matching.DefaultLongPollTimeout)
}

func (cf *rpcClientFactory) NewFrontendClient() (frontend.Client, error) {
	return cf.NewFrontendClientWithTimeout(frontend.DefaultTimeout)
}

func (cf *rpcClientFactory) NewHistoryClientWithTimeout(timeout time.Duration) (history.Client, error) {
	client, err := history.NewClient(cf.df, cf.monitor, cf.numberOfHistoryShards, timeout)
	if err != nil {
		return nil, err
	}
	if cf.metricsClient != nil {
		client = history.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewMatchingClientWithTimeout(
	timeout time.Duration,
	longPollTimeout time.Duration,
) (matching.Client, error) {

	client, err := matching.NewClient(cf.df, cf.monitor, timeout, longPollTimeout)
	if err != nil {
		return nil, err
	}
	if cf.metricsClient != nil {
		client = matching.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewFrontendClientWithTimeout(timeout time.Duration) (frontend.Client, error) {
	client, err := frontend.NewClient(cf.df, cf.monitor, timeout)
	if err != nil {
		return nil, err
	}
	if cf.metricsClient != nil {
		client = frontend.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}
