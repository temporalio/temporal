package common

import (
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/client/matching"
	"code.uber.internal/devexp/minions/common/membership"
	tchannel "github.com/uber/tchannel-go"
)

// ClientFactory can be used to create RPC clients for cadence services
type ClientFactory interface {
	NewHistoryClient() (history.Client, error)
	NewMatchingClient() (matching.Client, error)
}

type tchannelClientFactory struct {
	ch      *tchannel.Channel
	monitor membership.Monitor
}

func newTChannelClientFactory(ch *tchannel.Channel, monitor membership.Monitor) ClientFactory {
	return &tchannelClientFactory{
		ch:      ch,
		monitor: monitor,
	}
}

func (cf *tchannelClientFactory) NewHistoryClient() (history.Client, error) {
	return history.NewClient(cf.ch, cf.monitor)
}

func (cf *tchannelClientFactory) NewMatchingClient() (matching.Client, error) {
	return matching.NewClient(cf.ch, cf.monitor)
}
