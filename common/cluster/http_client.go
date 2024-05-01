package cluster

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/collection"
)

type tlsConfigProvider interface {
	GetRemoteClusterClientConfig(hostname string) (*tls.Config, error)
}

type HttpClient struct {
	http.Client
	Address string
}

type HttpClientCache struct {
	metadata    Metadata
	tlsProvider tlsConfigProvider
	clients     collection.SyncMap[string, *HttpClient]
}

func NewHttpClientCache(
	metadata Metadata,
	tlsProvider tlsConfigProvider,
) *HttpClientCache {
	cache := &HttpClientCache{
		metadata:    metadata,
		tlsProvider: tlsProvider,
		clients:     collection.NewSyncMap[string, *HttpClient](),
	}
	metadata.RegisterMetadataChangeCallback(cache, cache.evictionCallback)
	return cache
}

// Get returns a cached HttpClient if available, or constructs a new one for the given cluster name.
func (c *HttpClientCache) Get(targetClusterName string) (*HttpClient, error) {
	client, ok := c.clients.Get(targetClusterName)
	if ok {
		return client, nil
	}

	client, err := c.newClientForCluster(targetClusterName)
	if err != nil {
		return nil, err
	}

	c.clients.Set(targetClusterName, client)
	return client, nil
}

func (c *HttpClientCache) newClientForCluster(targetClusterName string) (*HttpClient, error) {
	targetInfo, ok := c.metadata.GetAllClusterInfo()[targetClusterName]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find cluster metadata for cluster %s", targetClusterName))
	}

	address, err := url.Parse(targetInfo.HTTPAddress)
	if err != nil {
		return nil, err
	}

	client := http.Client{}

	if c.tlsProvider != nil {
		tlsClientConfig, err := c.tlsProvider.GetRemoteClusterClientConfig(address.Hostname())
		if err != nil {
			return nil, err
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig}
	}

	return &HttpClient{
		Address: targetInfo.HTTPAddress,
		Client:  client,
	}, nil
}

// evictionCallback is invoked by cluster.Metadata when cluster information changes.
// It invalidates clients which are either no longer present or have had their HTTP address changed.
// It is assumed that TLS information has not changed for clusters that are unmodified.
func (c *HttpClientCache) evictionCallback(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation) {
	for oldClusterName, oldClusterInfo := range oldClusterMetadata {
		if oldClusterName == c.metadata.GetCurrentClusterName() || oldClusterInfo == nil {
			continue
		}

		newClusterInfo, exists := newClusterMetadata[oldClusterName]
		if !exists || oldClusterInfo.HTTPAddress != newClusterInfo.HTTPAddress {
			// Cluster was removed or had its HTTP address changed, so invalidate the cached client for that cluster.
			client, ok := c.clients.Pop(oldClusterName)
			if ok {
				client.CloseIdleConnections()
			}
		}
	}
}
