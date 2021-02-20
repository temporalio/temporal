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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination clientBean_mock.go

package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common/cluster"
)

type (
	// Bean in an collection of clients
	Bean interface {
		GetHistoryClient() historyservice.HistoryServiceClient
		SetHistoryClient(client historyservice.HistoryServiceClient)
		GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error)
		SetMatchingClient(client matchingservice.MatchingServiceClient)
		GetFrontendClient() workflowservice.WorkflowServiceClient
		SetFrontendClient(client workflowservice.WorkflowServiceClient)
		GetRemoteAdminClient(cluster string) adminservice.AdminServiceClient
		SetRemoteAdminClient(cluster string, client adminservice.AdminServiceClient)
		GetRemoteFrontendClient(cluster string) workflowservice.WorkflowServiceClient
		SetRemoteFrontendClient(cluster string, client workflowservice.WorkflowServiceClient)
	}

	clientBeanImpl struct {
		sync.Mutex
		currentCluster        string
		historyClient         historyservice.HistoryServiceClient
		matchingClient        atomic.Value
		remoteAdminClients    map[string]adminservice.AdminServiceClient
		remoteFrontendClients map[string]workflowservice.WorkflowServiceClient
		factory               Factory
	}
)

// NewClientBean provides a collection of clients
func NewClientBean(factory Factory, clusterMetadata cluster.Metadata) (Bean, error) {

	historyClient, err := factory.NewHistoryClient()
	if err != nil {
		return nil, err
	}

	remoteAdminClients := map[string]adminservice.AdminServiceClient{}
	remoteFrontendClients := map[string]workflowservice.WorkflowServiceClient{}

	for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		adminClient, err := factory.NewAdminClientWithTimeout(
			info.RPCAddress,
			admin.DefaultTimeout,
			admin.DefaultLargeTimeout,
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

func (h *clientBeanImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return h.historyClient
}

func (h *clientBeanImpl) SetHistoryClient(
	client historyservice.HistoryServiceClient,
) {
	h.historyClient = client
}

func (h *clientBeanImpl) GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error) {
	if client := h.matchingClient.Load(); client != nil {
		return client.(matchingservice.MatchingServiceClient), nil
	}
	return h.lazyInitMatchingClient(namespaceIDToName)
}

func (h *clientBeanImpl) SetMatchingClient(
	client matchingservice.MatchingServiceClient,
) {
	h.matchingClient.Store(client)
}

func (h *clientBeanImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return h.remoteFrontendClients[h.currentCluster]
}

func (h *clientBeanImpl) SetFrontendClient(
	client workflowservice.WorkflowServiceClient,
) {
	h.remoteFrontendClients[h.currentCluster] = client
}

func (h *clientBeanImpl) GetRemoteAdminClient(cluster string) adminservice.AdminServiceClient {
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
	client adminservice.AdminServiceClient,
) {
	h.remoteAdminClients[cluster] = client
}

func (h *clientBeanImpl) GetRemoteFrontendClient(cluster string) workflowservice.WorkflowServiceClient {
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
	client workflowservice.WorkflowServiceClient,
) {
	h.remoteFrontendClients[cluster] = client
}

func (h *clientBeanImpl) lazyInitMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error) {
	h.Lock()
	defer h.Unlock()
	if cached := h.matchingClient.Load(); cached != nil {
		return cached.(matchingservice.MatchingServiceClient), nil
	}
	client, err := h.factory.NewMatchingClient(namespaceIDToName)
	if err != nil {
		return nil, err
	}
	h.matchingClient.Store(client)
	return client, nil
}
