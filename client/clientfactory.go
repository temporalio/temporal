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
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	tchannel "github.com/uber/tchannel-go"
)

// Factory can be used to create RPC clients for cadence services
type Factory interface {
	NewHistoryClient() (history.Client, error)
	NewMatchingClient() (matching.Client, error)
}

type tchannelClientFactory struct {
	ch                    *tchannel.Channel
	monitor               membership.Monitor
	metricsClient         metrics.Client
	numberOfHistoryShards int
}

// NewTChannelClientFactory creates an instance of client factory using tchannel
func NewTChannelClientFactory(ch *tchannel.Channel,
	monitor membership.Monitor, metricsClient metrics.Client, numberOfHistoryShards int) Factory {
	return &tchannelClientFactory{
		ch:                    ch,
		monitor:               monitor,
		metricsClient:         metricsClient,
		numberOfHistoryShards: numberOfHistoryShards,
	}
}

func (cf *tchannelClientFactory) NewHistoryClient() (history.Client, error) {
	client, err := history.NewClient(cf.ch, cf.monitor, cf.numberOfHistoryShards)
	if err != nil {
		return nil, err
	}
	if cf.metricsClient != nil {
		client = history.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}

func (cf *tchannelClientFactory) NewMatchingClient() (matching.Client, error) {
	client, err := matching.NewClient(cf.ch, cf.monitor)
	if err != nil {
		return nil, err
	}
	if cf.metricsClient != nil {
		client = matching.NewMetricClient(client, cf.metricsClient)
	}
	return client, nil
}
