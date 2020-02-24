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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination clientBean_mock.go -self_package github.com/temporalio/temporal/client

package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/frontend"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common/cluster"
)

type (
	// Bean in an collection of clients
	Bean interface {
		GetHistoryClient() history.Client
		SetHistoryClient(client history.Client)
		GetMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error)
		SetMatchingClient(client matching.Client)
		GetFrontendClient() frontend.Client
		SetFrontendClient(client frontend.Client)
		GetRemoteAdminClient(cluster string) admin.Client
		SetRemoteAdminClient(cluster string, client admin.Client)
		GetRemoteFrontendClient(cluster string) frontend.Client
		SetRemoteFrontendClient(cluster string, client frontend.Client)
	}

	clientBeanImpl struct {
		sync.Mutex
		currentCluster        string
		historyClient         history.Client
		matchingClient        atomic.Value
		remoteAdminClients    map[string]admin.Client
		remoteFrontendClients map[string]frontend.Client
		factory               Factory
	}
)

// NewClientBean provides a collection of clients
func NewClientBean(factory Factory, clusterMetadata cluster.Metadata) (Bean, error) {

	historyClient, err := factory.NewHistoryClient()
	if err != nil {
		return nil, err
	}

	remoteAdminClients := map[string]admin.Client{}
	remoteFrontendClients := map[string]frontend.Client{}

	for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		adminClient, err := factory.NewAdminClientWithTimeout(
			info.RPCAddress,
			admin.DefaultTimeout,
		)
		if err != nil {
			return nil, err
		}

		remoteFrontendClient, err := factory.NewFrontendClientWithTimeout(
			info.RPCAddress,
			frontend.DefaultTimeout,
			frontend.DefaultLongPollTimeout,
		)
		if err != nil {
			return nil, err
		}

		remoteAdminClients[clusterName] = adminClient
		remoteFrontendClients[clusterName] = remoteFrontendClient
	}

	return &clientBeanImpl{
		currentCluster:        clusterMetadata.GetCurrentClusterName(),
		factory:               factory,
		historyClient:         historyClient,
		remoteAdminClients:    remoteAdminClients,
		remoteFrontendClients: remoteFrontendClients,
	}, nil
}

func (h *clientBeanImpl) GetHistoryClient() history.Client {
	return h.historyClient
}

func (h *clientBeanImpl) SetHistoryClient(
	client history.Client,
) {
	h.historyClient = client
}

func (h *clientBeanImpl) GetMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error) {
	if client := h.matchingClient.Load(); client != nil {
		return client.(matching.Client), nil
	}
	return h.lazyInitMatchingClient(domainIDToName)
}

func (h *clientBeanImpl) SetMatchingClient(
	client matching.Client,
) {
	h.matchingClient.Store(client)
}

func (h *clientBeanImpl) GetFrontendClient() frontend.Client {
	return h.remoteFrontendClients[h.currentCluster]
}

func (h *clientBeanImpl) SetFrontendClient(
	client frontend.Client,
) {
	h.remoteFrontendClients[h.currentCluster] = client
}

func (h *clientBeanImpl) GetRemoteAdminClient(cluster string) admin.Client {
	client, ok := h.remoteAdminClients[cluster]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster client map: %v.",
			cluster,
			h.remoteAdminClients,
		))
	}
	return client
}

func (h *clientBeanImpl) SetRemoteAdminClient(
	cluster string,
	client admin.Client,
) {
	h.remoteAdminClients[cluster] = client
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

func (h *clientBeanImpl) SetRemoteFrontendClient(
	cluster string,
	client frontend.Client,
) {
	h.remoteFrontendClients[cluster] = client
}

func (h *clientBeanImpl) lazyInitMatchingClient(domainIDToName DomainIDToNameFunc) (matching.Client, error) {
	h.Lock()
	defer h.Unlock()
	if cached := h.matchingClient.Load(); cached != nil {
		return cached.(matching.Client), nil
	}
	client, err := h.factory.NewMatchingClient(domainIDToName)
	if err != nil {
		return nil, err
	}
	h.matchingClient.Store(client)
	return client, nil
}
