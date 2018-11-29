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
	"errors"
	"fmt"
	"regexp"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"

	"github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
)

const (
	ipPortRegex = `\b(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9]):[1-9]\d*\b`
)

type (
	// Bean in an collection of clients
	Bean interface {
		GetHistoryClient() history.Client
		GetMatchingClient() matching.Client
		GetFrontendClient() frontend.Client
		GetRemoteFrontendClient(cluster string) frontend.Client
	}

	// DispatcherProvider provides a diapatcher to a given address
	DispatcherProvider interface {
		Get(address string) (*yarpc.Dispatcher, error)
	}

	clientBeanImpl struct {
		historyClient         history.Client
		matchingClient        matching.Client
		frontendClient        frontend.Client
		remoteFrontendClients map[string]frontend.Client
	}

	ipDispatcherProvider struct {
	}
)

// NewClientBean provides a collection of clients
func NewClientBean(factory Factory, dispatcherProvider DispatcherProvider, clusterMetadata cluster.Metadata) (Bean, error) {

	historyClient, err := factory.NewHistoryClient()
	if err != nil {
		return nil, err
	}

	matchingClient, err := factory.NewMatchingClient()
	if err != nil {
		return nil, err
	}

	frontendClient, err := factory.NewFrontendClient()
	if err != nil {
		return nil, err
	}

	remoteFrontendClients := map[string]frontend.Client{}
	for cluster, address := range clusterMetadata.GetAllClientAddress() {
		dispatcher, err := dispatcherProvider.Get(address)
		if err != nil {
			return nil, err
		}

		client, err := factory.NewFrontendClientWithTimeoutAndDispatcher(
			frontend.DefaultTimeout,
			frontend.DefaultLongPollTimeout,
			dispatcher,
		)
		if err != nil {
			return nil, err
		}

		remoteFrontendClients[cluster] = client
	}

	return &clientBeanImpl{
		historyClient:         historyClient,
		matchingClient:        matchingClient,
		frontendClient:        frontendClient,
		remoteFrontendClients: remoteFrontendClients,
	}, nil
}

func (h *clientBeanImpl) GetHistoryClient() history.Client {
	return h.historyClient
}

func (h *clientBeanImpl) GetMatchingClient() matching.Client {
	return h.matchingClient
}

func (h *clientBeanImpl) GetFrontendClient() frontend.Client {
	return h.frontendClient
}

func (h *clientBeanImpl) GetRemoteFrontendClient(cluster string) frontend.Client {
	client, ok := h.remoteFrontendClients[cluster]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster client map: %v.",
			cluster,
			h.remoteFrontendClients,
		))
	}
	return client
}

// NewIPYarpcDispatcherProvider create a dispatcher provider which handles with IP address
func NewIPYarpcDispatcherProvider() DispatcherProvider {
	return &ipDispatcherProvider{}
}

func (p *ipDispatcherProvider) Get(address string) (*yarpc.Dispatcher, error) {
	match, err := regexp.MatchString(ipPortRegex, address)
	if err != nil {
		return nil, err
	}
	if !match {
		return nil, errors.New("invalid ip:port address")
	}

	channel, err := tchannel.NewChannelTransport(tchannel.ServiceName(crossDCCaller))
	if err != nil {
		return nil, err
	}

	return yarpc.NewDispatcher(yarpc.Config{
		Name: crossDCCaller,
		Outbounds: yarpc.Outbounds{
			common.FrontendServiceName: {Unary: channel.NewSingleOutbound(address)},
		},
	}), nil
}
