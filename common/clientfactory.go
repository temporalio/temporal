package common

import (
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/client/matching"
	tchannel "github.com/uber/tchannel-go"
)

// ClientFactory can be used to create RPC clients for cadence services
type ClientFactory interface {
	NewHistoryClient() (history.Client, error)
	NewMatchingClient() (matching.Client, error)
}

type tchannelClientFactory struct {
	ch *tchannel.Channel
}

func newTChannelClientFactory(ch *tchannel.Channel) ClientFactory {
	return &tchannelClientFactory{
		ch: ch,
	}
}

func (cf *tchannelClientFactory) NewHistoryClient() (history.Client, error) {
	return history.NewClient(cf.ch)
}

func (cf *tchannelClientFactory) NewMatchingClient() (matching.Client, error) {
	return matching.NewClient(cf.ch)
}
