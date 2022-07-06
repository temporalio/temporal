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

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common/cluster"
)

type (
	// Bean is a collection of clients
	Bean interface {
		GetHistoryClient() historyservice.HistoryServiceClient
		SetHistoryClient(client historyservice.HistoryServiceClient)
		GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error)
		SetMatchingClient(client matchingservice.MatchingServiceClient)
		GetFrontendClient() workflowservice.WorkflowServiceClient
		SetFrontendClient(client workflowservice.WorkflowServiceClient)
		GetRemoteAdminClient(cluster string) (adminservice.AdminServiceClient, error)
		SetRemoteAdminClient(cluster string, client adminservice.AdminServiceClient)
		GetRemoteFrontendClient(cluster string) (workflowservice.WorkflowServiceClient, error)
	}

	clientBeanImpl struct {
		sync.Mutex
		historyClient   historyservice.HistoryServiceClient
		matchingClient  atomic.Value
		clusterMetadata cluster.Metadata
		factory         Factory

		remoteAdminClientsLock    sync.RWMutex
		remoteAdminClients        map[string]adminservice.AdminServiceClient
		remoteFrontendClientsLock sync.RWMutex
		remoteFrontendClients     map[string]workflowservice.WorkflowServiceClient
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

		adminClient := factory.NewAdminClientWithTimeout(
			info.RPCAddress,
			admin.DefaultTimeout,
			admin.DefaultLargeTimeout,
		)

		remoteFrontendClient := factory.NewFrontendClientWithTimeout(
			info.RPCAddress,
			frontend.DefaultTimeout,
			frontend.DefaultLongPollTimeout,
		)

		remoteAdminClients[clusterName] = adminClient
		remoteFrontendClients[clusterName] = remoteFrontendClient
	}

	bean := &clientBeanImpl{
		factory:               factory,
		historyClient:         historyClient,
		clusterMetadata:       clusterMetadata,
		remoteAdminClients:    remoteAdminClients,
		remoteFrontendClients: remoteFrontendClients,
	}
	bean.registerClientEviction()
	return bean, nil
}

func (h *clientBeanImpl) registerClientEviction() {
	currentCluster := h.clusterMetadata.GetCurrentClusterName()
	h.clusterMetadata.RegisterMetadataChangeCallback(
		h,
		func(oldClusterMetadata map[string]*cluster.ClusterInformation, newClusterMetadata map[string]*cluster.ClusterInformation) {
			for clusterName := range newClusterMetadata {
				if clusterName == currentCluster {
					continue
				}
				h.remoteAdminClientsLock.Lock()
				delete(h.remoteAdminClients, clusterName)
				h.remoteAdminClientsLock.Unlock()
				h.remoteFrontendClientsLock.Lock()
				delete(h.remoteFrontendClients, clusterName)
				h.remoteFrontendClientsLock.Unlock()
			}
		})
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
	return h.remoteFrontendClients[h.clusterMetadata.GetCurrentClusterName()]
}

func (h *clientBeanImpl) SetFrontendClient(
	client workflowservice.WorkflowServiceClient,
) {
	h.remoteFrontendClients[h.clusterMetadata.GetCurrentClusterName()] = client
}

func (h *clientBeanImpl) GetRemoteAdminClient(cluster string) (adminservice.AdminServiceClient, error) {
	h.remoteAdminClientsLock.RLock()
	client, ok := h.remoteAdminClients[cluster]
	h.remoteAdminClientsLock.RUnlock()

	if !ok {
		clusterInfo, clusterFound := h.clusterMetadata.GetAllClusterInfo()[cluster]
		if !clusterFound {
			return nil, &serviceerror.Unavailable{
				Message: fmt.Sprintf(
					"Unknown cluster name: %v with given cluster information map: %v.",
					cluster,
					clusterInfo,
				),
			}
		}

		h.remoteAdminClientsLock.Lock()
		defer h.remoteAdminClientsLock.Unlock()
		client, ok = h.remoteAdminClients[cluster]
		if !ok {
			client = h.factory.NewAdminClientWithTimeout(
				clusterInfo.RPCAddress,
				admin.DefaultTimeout,
				admin.DefaultLargeTimeout,
			)
			h.setRemoteAdminClientLocked(cluster, client)
		}
	}
	return client, nil
}

func (h *clientBeanImpl) SetRemoteAdminClient(
	cluster string,
	client adminservice.AdminServiceClient,
) {
	h.remoteAdminClientsLock.Lock()
	defer h.remoteAdminClientsLock.Unlock()

	h.setRemoteAdminClientLocked(cluster, client)
}

func (h *clientBeanImpl) GetRemoteFrontendClient(cluster string) (workflowservice.WorkflowServiceClient, error) {
	h.remoteFrontendClientsLock.RLock()
	client, ok := h.remoteFrontendClients[cluster]
	h.remoteFrontendClientsLock.RUnlock()

	if !ok {
		clusterInfo, clusterFound := h.clusterMetadata.GetAllClusterInfo()[cluster]
		if !clusterFound {
			return nil, &serviceerror.Unavailable{
				Message: fmt.Sprintf(
					"Unknown cluster name: %v with given cluster information map: %v.",
					cluster,
					clusterInfo,
				),
			}
		}

		h.remoteFrontendClientsLock.Lock()
		defer h.remoteFrontendClientsLock.Unlock()
		client, ok = h.remoteFrontendClients[cluster]
		if !ok {
			client = h.factory.NewFrontendClientWithTimeout(
				clusterInfo.RPCAddress,
				frontend.DefaultTimeout,
				frontend.DefaultLongPollTimeout,
			)
			h.setRemoteFrontendClientLocked(cluster, client)
		}
	}
	return client, nil
}

func (h *clientBeanImpl) setRemoteFrontendClientLocked(
	cluster string,
	client workflowservice.WorkflowServiceClient,
) {
	h.remoteFrontendClients[cluster] = client
}

func (h *clientBeanImpl) setRemoteAdminClientLocked(
	cluster string,
	client adminservice.AdminServiceClient,
) {
	h.remoteAdminClients[cluster] = client
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
