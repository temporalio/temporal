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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination client_bean_mock.go

package client

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common/cluster"
)

type (
	// Bean is a collection of clients
	Bean interface {
		GetHistoryClient() historyservice.HistoryServiceClient
		GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error)
		GetFrontendClient() workflowservice.WorkflowServiceClient
		GetRemoteAdminClient(string) (adminservice.AdminServiceClient, error)
		SetRemoteAdminClient(string, adminservice.AdminServiceClient)
		GetRemoteFrontendClient(string) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error)
	}

	frontendClient struct {
		connection grpc.ClientConnInterface
		workflowservice.WorkflowServiceClient
	}

	clientBeanImpl struct {
		sync.Mutex
		historyClient   historyservice.HistoryServiceClient
		matchingClient  atomic.Value
		clusterMetadata cluster.Metadata
		factory         Factory

		adminClientsLock    sync.RWMutex
		adminClients        map[string]adminservice.AdminServiceClient
		frontendClientsLock sync.RWMutex
		frontendClients     map[string]frontendClient
	}
)

// NewClientBean provides a collection of clients
func NewClientBean(factory Factory, clusterMetadata cluster.Metadata) (Bean, error) {

	historyClient, err := factory.NewHistoryClientWithTimeout(history.DefaultTimeout)
	if err != nil {
		return nil, err
	}

	adminClients := map[string]adminservice.AdminServiceClient{}
	frontendClients := map[string]frontendClient{}

	currentClusterName := clusterMetadata.GetCurrentClusterName()
	// Init local cluster client with membership info
	adminClient, err := factory.NewLocalAdminClientWithTimeout(
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)
	if err != nil {
		return nil, err
	}
	conn, client, err := factory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return nil, err
	}
	adminClients[currentClusterName] = adminClient
	frontendClients[currentClusterName] = frontendClient{
		connection:            conn,
		WorkflowServiceClient: client,
	}

	for clusterName, info := range clusterMetadata.GetAllClusterInfo() {
		if !info.Enabled || clusterName == currentClusterName {
			continue
		}
		adminClient = factory.NewRemoteAdminClientWithTimeout(
			info.RPCAddress,
			admin.DefaultTimeout,
			admin.DefaultLargeTimeout,
		)
		conn, client = factory.NewRemoteFrontendClientWithTimeout(
			info.RPCAddress,
			frontend.DefaultTimeout,
			frontend.DefaultLongPollTimeout,
		)
		adminClients[clusterName] = adminClient
		frontendClients[clusterName] = frontendClient{
			connection:            conn,
			WorkflowServiceClient: client,
		}
	}

	bean := &clientBeanImpl{
		factory:         factory,
		historyClient:   historyClient,
		clusterMetadata: clusterMetadata,
		adminClients:    adminClients,
		frontendClients: frontendClients,
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
				h.adminClientsLock.Lock()
				delete(h.adminClients, clusterName)
				h.adminClientsLock.Unlock()
				h.frontendClientsLock.Lock()
				delete(h.frontendClients, clusterName)
				h.frontendClientsLock.Unlock()
			}
		})
}

func (h *clientBeanImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return h.historyClient
}

func (h *clientBeanImpl) GetMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error) {
	if client := h.matchingClient.Load(); client != nil {
		return client.(matchingservice.MatchingServiceClient), nil
	}
	return h.lazyInitMatchingClient(namespaceIDToName)
}

func (h *clientBeanImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return h.frontendClients[h.clusterMetadata.GetCurrentClusterName()]
}

func (h *clientBeanImpl) GetRemoteAdminClient(cluster string) (adminservice.AdminServiceClient, error) {
	h.adminClientsLock.RLock()
	client, ok := h.adminClients[cluster]
	h.adminClientsLock.RUnlock()
	if ok {
		return client, nil
	}

	clusterInfo, clusterFound := h.clusterMetadata.GetAllClusterInfo()[cluster]
	if !clusterFound {
		// We intentionally return internal error here.
		// This error could only happen with internal mis-configuration.
		// This can happen when a namespace is config for multiple clusters. But those clusters are not connected.
		// We also have logic in task processing to drop tasks when namespace cluster exclude a local cluster.
		return nil, &serviceerror.Internal{
			Message: fmt.Sprintf(
				"Unknown cluster name: %v with given cluster information map: %v.",
				cluster,
				clusterInfo,
			),
		}
	}

	h.adminClientsLock.Lock()
	defer h.adminClientsLock.Unlock()
	client, ok = h.adminClients[cluster]
	if ok {
		return client, nil
	}

	client = h.factory.NewRemoteAdminClientWithTimeout(
		clusterInfo.RPCAddress,
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)
	h.adminClients[cluster] = client
	return client, nil
}

func (h *clientBeanImpl) SetRemoteAdminClient(
	cluster string,
	client adminservice.AdminServiceClient,
) {
	h.adminClientsLock.Lock()
	defer h.adminClientsLock.Unlock()

	h.adminClients[cluster] = client
}

func (h *clientBeanImpl) GetRemoteFrontendClient(clusterName string) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error) {
	h.frontendClientsLock.RLock()
	client, ok := h.frontendClients[clusterName]
	h.frontendClientsLock.RUnlock()
	if ok {
		return client.connection, client, nil
	}

	clusterInfo, clusterFound := h.clusterMetadata.GetAllClusterInfo()[clusterName]
	if !clusterFound {
		// We intentionally return internal error here.
		// This error could only happen with internal mis-configuration.
		// This can happen when a namespace is config for multiple clusters. But those clusters are not connected.
		// We also have logic in task processing to drop tasks when namespace cluster exclude a local cluster.
		return nil, nil, &serviceerror.Internal{
			Message: fmt.Sprintf(
				"Unknown clusterName name: %v with given clusterName information map: %v.",
				clusterName,
				clusterInfo,
			),
		}
	}

	h.frontendClientsLock.Lock()
	defer h.frontendClientsLock.Unlock()

	client, ok = h.frontendClients[clusterName]
	if ok {
		return client.connection, client, nil
	}

	conn, fClient := h.factory.NewRemoteFrontendClientWithTimeout(
		clusterInfo.RPCAddress,
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	client = frontendClient{
		connection:            conn,
		WorkflowServiceClient: fClient,
	}
	h.frontendClients[clusterName] = client
	return client.connection, client, nil
}

func (h *clientBeanImpl) setRemoteAdminClientLocked(
	cluster string,
	client adminservice.AdminServiceClient,
) {
	h.adminClients[cluster] = client
}

func (h *clientBeanImpl) lazyInitMatchingClient(namespaceIDToName NamespaceIDToNameFunc) (matchingservice.MatchingServiceClient, error) {
	h.Lock()
	defer h.Unlock()
	if cached := h.matchingClient.Load(); cached != nil {
		return cached.(matchingservice.MatchingServiceClient), nil
	}
	client, err := h.factory.NewMatchingClientWithTimeout(namespaceIDToName, matching.DefaultTimeout, matching.DefaultLongPollTimeout)
	if err != nil {
		return nil, err
	}
	h.matchingClient.Store(client)
	return client, nil
}
