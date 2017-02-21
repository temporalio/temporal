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
