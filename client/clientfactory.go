package client

import (
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common/membership"
	"code.uber.internal/devexp/minions/common/metrics"
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
	return history.NewClient(cf.ch, cf.monitor, cf.numberOfHistoryShards)
}

func (cf *tchannelClientFactory) NewMatchingClient() (matching.Client, error) {
	return matching.NewClient(cf.ch, cf.monitor)
}
